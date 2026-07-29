import logging
import requests
import json
import asyncio
from datetime import timedelta
from homeassistant.const import (
    UnitOfEnergy,
    UnitOfFrequency,
    UnitOfElectricCurrent,
    UnitOfElectricPotential,
    UnitOfApparentPower,
    UnitOfPower,
    PERCENTAGE,
    SIGNAL_STRENGTH_DECIBELS,
)
try:
    from homeassistant.const import UnitOfReactivePower
    POWER_VOLT_AMPERE_REACTIVE = UnitOfReactivePower.VOLT_AMPERE_REACTIVE
except (ImportError, AttributeError):
    from homeassistant.const import POWER_VOLT_AMPERE_REACTIVE

from homeassistant.helpers.entity import DeviceInfo
from homeassistant.components.sensor import SensorEntity, SensorDeviceClass, SensorStateClass
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)
from packaging import version
from .const import DOMAIN, CONF_HOST_IP, DEFAULT_TIMEOUT

_LOGGER = logging.getLogger(__name__)

SETTINGS_FILE = "custom_components/bituopmd/settings.json"
OTA_VERSIONS_FILE = "custom_components/bituopmd/ota_versions.json"

def load_settings():
    try:
        with open(SETTINGS_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        _LOGGER.warning("Settings file not found, using default values.")
        return {"devices": {}}
    except json.JSONDecodeError as err:
        _LOGGER.error(f"Error decoding settings file: {err}")
        return {"devices": {}}

def save_settings(settings):
    try:
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings, f)
    except IOError as err:
        _LOGGER.error(f"Error saving settings: {err}")

def load_ota_versions():
    try:
        with open(OTA_VERSIONS_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        _LOGGER.warning("OTA versions file not found.")
        return {}
    except json.JSONDecodeError as err:
        _LOGGER.error(f"Error decoding OTA versions file: {err}")
        return {}

settings = load_settings()


UNIT_MAPPING = {
    "unbalancelinecurrents": PERCENTAGE,
    "powerfactor": None,
    "voltage": UnitOfElectricPotential.VOLT,
    "current": UnitOfElectricCurrent.AMPERE,
    "energy": UnitOfEnergy.KILO_WATT_HOUR,
    "apparent": UnitOfApparentPower.VOLT_AMPERE,
    "reactive": POWER_VOLT_AMPERE_REACTIVE,
    "power": UnitOfPower.WATT,
    "frequency": UnitOfFrequency.HERTZ,
    "rssi": SIGNAL_STRENGTH_DECIBELS,
}

STATE_CLASSES = {
    "voltage": SensorStateClass.MEASUREMENT,
    "current": SensorStateClass.MEASUREMENT,
    "power": SensorStateClass.MEASUREMENT,
    "energy": SensorStateClass.TOTAL_INCREASING,
    "frequency": SensorStateClass.MEASUREMENT,
    "rssi": SensorStateClass.MEASUREMENT,
}

EXCLUDE_FIELDS = {"Post", "Time", "Config485", "MqttStatus", "ProductModel", "IP", "SerialNumber", "DeviceType", "FWVersion", "MCUVersion", "Manufactor"}

async def async_setup_entry(hass, entry, async_add_entities):
    """Set up sensor platform."""
    host_ip = entry.data[CONF_HOST_IP]
    current_scan_interval = settings["devices"].get(host_ip, {}).get("scan_interval", 5)
    # Load OTA version info in the executor to avoid blocking the event loop
    ota_versions = await hass.async_add_executor_job(load_ota_versions)
    coordinator = BituoDataUpdateCoordinator(hass, host_ip, current_scan_interval, ota_versions)
    # Store the coordinator so it can be accessed by other platforms like button
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        'sensor_coordinator': coordinator
    }
    await coordinator.async_config_entry_first_refresh()

    # Fetch device model and firmware version
    try:
        device_info = await coordinator.fetch_device_info()
    except UpdateFailed:
        _LOGGER.error("Failed to fetch device info for %s", host_ip)
        device_info = {}

    # Create sensor entities for each data field
    sensors = [
        BituoSensor(coordinator, host_ip, field, device_info.get("model", "Unknown Model"), device_info.get("fw_version", "Unknown"), device_info.get("manufacturer", "Unknown"), device_info.get("mcu_version", "Unknown"))
        for field in coordinator.data.keys()
        if field not in EXCLUDE_FIELDS
    ]
    ota_sensor = BituoOTASensor(coordinator, host_ip, device_info.get("model", "Unknown Model"), device_info.get("fw_version", "Unknown"), device_info.get("manufacturer", "Unknown"), device_info.get("mcu_version", "Unknown"))
    sensors.append(ota_sensor)

    # Assign the OTA sensor to the coordinator
    coordinator.ota_entity = ota_sensor

    async_add_entities(sensors, True)

    async def handle_set_frequency(call):
        """Handle the service call to set the data fetch frequency."""
        frequency = call.data.get("frequency", 5)
        device_id = call.data.get("device_id")  # host IP; empty = all devices

        # Apply to every configured device when no explicit target is given,
        # otherwise only to the matching one.
        targets = []
        for entry_id, bucket in hass.data.get(DOMAIN, {}).items():
            coord = bucket.get("sensor_coordinator") if isinstance(bucket, dict) else None
            if coord is None:
                continue
            if device_id in (None, "", coord.host_ip):
                targets.append(coord)

        for coord in targets:
            settings["devices"].setdefault(coord.host_ip, {})["scan_interval"] = frequency
            coord.update_interval = timedelta(seconds=frequency)

        await hass.async_add_executor_job(save_settings, settings)

        for coord in targets:
            await coord.async_refresh()

    hass.services.async_register(DOMAIN, "set_frequency", handle_set_frequency)

class BituoDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching data from the device."""

    def __init__(self, hass, host_ip, scan_interval, ota_versions=None):
        """Initialize."""
        self.host_ip = host_ip
        self.ota_versions = ota_versions if ota_versions is not None else {}
        self.ota_entity = None  # init ota_entity
        super().__init__(hass, _LOGGER, name=DOMAIN, update_interval=timedelta(seconds=scan_interval))

        self._tasks = [
            self.hass.loop.create_task(self._periodically_update_scan_interval()),
            # Schedule OTA update check every 30 minutes
            self.hass.loop.create_task(self._schedule_ota_update_checks()),
        ]

    async def async_dispose(self):
        """Cancel background tasks when the config entry is unloaded."""
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

    def get_scan_interval(self):
        """Get the scan interval from settings."""
        return settings["devices"].get(self.host_ip, {}).get("scan_interval", 5)
    
    async def _periodically_update_scan_interval(self):
        """Periodically update the scan interval from settings.json."""
        while True:
            # 读取settings中的最新扫描间隔
            new_interval = self.get_scan_interval()
            
            # 检查是否需要更新coordinator的扫描间隔
            if new_interval != self.update_interval.total_seconds():
                _LOGGER.info(f"Updating scan interval for {self.host_ip} to {new_interval} seconds")
                self.update_interval = timedelta(seconds=new_interval)
            
            # 等待一段时间后再检查
            await asyncio.sleep(60)  # 每分钟检查一次


    async def _async_update_data(self):
        """Fetch data from the device."""
        try:
            response = await self.hass.async_add_executor_job(
                lambda: requests.get(f"http://{self.host_ip}/data", timeout=DEFAULT_TIMEOUT)
            )
            response.raise_for_status()
            data = response.json()
            # _LOGGER.debug(f"{self.host_ip} - {data}")
            
            # Multiply power values by 1000
            for key in data:
                if 'power' in key.lower() and 'factor' not in key.lower():
                    try:
                        data[key] = float(data[key]) * 1000
                    except ValueError:
                        _LOGGER.error(f"Non-numeric value found for key {key}: {data[key]}")
                        data[key] = None

            return data
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}")

    async def fetch_device_info(self):
        """Fetch device model and firmware version information."""
        try:
            response = await self.hass.async_add_executor_job(
                lambda: requests.get(f"http://{self.host_ip}/data", timeout=DEFAULT_TIMEOUT)
            )
            response.raise_for_status()
            data = response.json()
            return {
                "model": data.get("ProductModel", "Unknown Model"),
                "fw_version": data.get("FWVersion", "Unknown"),
                "manufacturer": "BITUO TECHNIK",
                "mcu_version": data.get("MCUVersion", "Unknown"),
            }
        except Exception as err:
            raise UpdateFailed(f"Error fetching device info: {err}")

    async def check_for_ota_updates(self):
        """Check for OTA updates."""
        try:
            if self.ota_entity is None:
                return

            device_info = await self.fetch_device_info()
            current_version = device_info.get("fw_version", "Unknown")

            if current_version != "Unknown" and int(current_version.split('.')[0]) >= 4:
                latest_version = self.ota_versions.get("common", "Unknown")
            else:
                model = device_info.get("model", "Unknown Model")
                latest_version = self.ota_versions.get(model, "Unknown")

            if latest_version != "Unknown" and current_version != "Unknown" and version.parse(current_version) < version.parse(latest_version):
                self.ota_entity._attr_state = "OTA Available"
            else:
                self.ota_entity._attr_state = "Up to Date"
            self.ota_entity.async_write_ha_state()
        except Exception as err:
            _LOGGER.error(f"Error checking OTA updates: {err}")
    
    async def _schedule_ota_update_checks(self):
        """Schedule periodic OTA update checks."""
        while True:
            await self.check_for_ota_updates()
            await asyncio.sleep(1800)  # Wait for 30 minutes

class BituoSensor(CoordinatorEntity, SensorEntity):
    """Representation of a Sensor."""

    def __init__(self, coordinator, host_ip, field, model, fw_version, manufacturer, mcu_version):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._field = field
        self._attr_name = self.format_field_name(field)
        self._attr_unique_id = f"{host_ip}_{field}"
        self.entity_id = f"sensor.{host_ip.replace('.', '_')}_{self.format_field_entity_id(field)}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, host_ip)},
            name=f"{model} - {host_ip}",
            manufacturer=manufacturer,
            model=model,
            sw_version=f"S{fw_version}_M{self.format_version(mcu_version)}",
            configuration_url=f"http://{host_ip}"  # embed URL
        )
        self._host_ip = host_ip
        self._native_unit_of_measurement = self.get_initial_unit_of_measurement()
        self._attr_state_class = self.get_state_class()
        self._attr_device_class = self.get_device_class()

        # Set default precision for power data
        if "power" in self._field.lower() and "active" in self._field.lower():
            self._attr_suggested_display_precision = 0
        if "power" in self._field.lower() and "apparent" in self._field.lower():
            self._attr_suggested_display_precision = 0
        if "unbalancelinecurrents" in self._field.lower():
            self._attr_suggested_display_precision = 0

    def get_initial_unit_of_measurement(self):
        """Determine the initial unit of measurement based on the field."""
        for unit_keyword, unit in UNIT_MAPPING.items():
            if unit_keyword in self._field.lower():
                return unit
        return None

    def get_state_class(self):
        """Determine the state class based on the field."""
        for state_class_keyword, state_class in STATE_CLASSES.items():
            if state_class_keyword in self._field.lower():
                return state_class
        return SensorStateClass.MEASUREMENT  # Default state class

    def get_device_class(self):
        """Determine the device class based on the field."""
        if "unbalancelinecurrents" in self._field.lower():
            return SensorDeviceClass.POWER_FACTOR
        if "power" in self._field.lower():
            if "factor" in self._field.lower():
                return SensorDeviceClass.POWER_FACTOR
            elif "reactive" in self._field.lower():
                return SensorDeviceClass.REACTIVE_POWER
            elif "apparent" in self._field.lower():
                return SensorDeviceClass.APPARENT_POWER
            elif "active" in self._field.lower():
                return SensorDeviceClass.POWER
        elif "energy" in self._field.lower():
            return SensorDeviceClass.ENERGY
        elif "current" in self._field.lower():
            return SensorDeviceClass.CURRENT
        elif "voltage" in self._field.lower():
            return SensorDeviceClass.VOLTAGE
        elif "frequency" in self._field.lower():
            return SensorDeviceClass.FREQUENCY
        elif "rssi" in self._field.lower():
            return SensorDeviceClass.SIGNAL_STRENGTH
        return None

    @staticmethod
    def format_field_name(field):
        """Format field name to be more readable."""
        formatted_name = ''.join([' ' + char if char.isupper() else char for char in field]).title().strip()
        formatted_name = formatted_name.replace("X", " X").replace("Y", " Y").replace("Z", " Z")
        return formatted_name
    
    @staticmethod
    def format_field_entity_id(field):
        """Format field name to be more suitable for unique_id."""
        import re
        
        formatted_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', field).lower()
        
        formatted_name = re.sub(r'_+', '_', formatted_name)
        
        return formatted_name.strip('_')

    @staticmethod
    def format_version(version):
        if version.lower() == "unknown":
            return version 
        parts = version.split('.')
        formatted_parts = [] 
        for part in parts:
            if part.strip():  # 检查部分是否为空
                try:
                    formatted_parts.append(str(int(part)))
                except ValueError:
                    formatted_parts.append('unknown')
            else:
                formatted_parts.append('unknown')  # 如果部分为空，设置为 'unknown'
        
        formatted_version = '.'.join(formatted_parts)
        return formatted_version

    @property
    def native_value(self):
        """Return the native state of the sensor."""
        # None keeps the entity unavailable instead of injecting bogus 0
        # readings into long-term statistics (critical for energy sensors).
        return self.coordinator.data.get(self._field)

    @property
    def native_unit_of_measurement(self):
        """Return the native unit of measurement."""
        return self._native_unit_of_measurement

    @property
    def device_class(self):
        """Return the class of this device."""
        return self._attr_device_class

class BituoOTASensor(CoordinatorEntity, SensorEntity):
    """Representation of an OTA status sensor."""

    def __init__(self, coordinator, host_ip, model, fw_version, manufacturer, mcu_version):
        """Initialize the OTA status sensor."""
        super().__init__(coordinator)
        self._attr_name = "OTA Status"
        self._attr_unique_id = f"{host_ip}_ota_status"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, host_ip)},
            name=f"{model} - {host_ip}",
            manufacturer=manufacturer,
            model=model,
            sw_version=f"S{fw_version}_M{self.format_version(mcu_version)}",
            configuration_url=f"http://{host_ip}"  # embed URL
        )
        self._host_ip = host_ip
        self._attr_state = "unknown"
        coordinator.ota_entity = self  # 存储自身的引用到协调器中
        self._attr_icon = "mdi:update"
        self.entity_id = f"sensor.{self._attr_unique_id.replace('.', '_')}"
    
    @staticmethod
    def format_version(version):
        if version.lower() == "unknown":
            return version 
        parts = version.split('.')
        formatted_parts = [] 
        for part in parts:
            if part.strip():  # 检查部分是否为空
                try:
                    formatted_parts.append(str(int(part)))
                except ValueError:
                    formatted_parts.append('unknown')
            else:
                formatted_parts.append('unknown')  # 如果部分为空，设置为 'unknown'
        
        formatted_version = '.'.join(formatted_parts)
        return formatted_version

    @property
    def native_value(self):
        """Return the native state of the sensor."""
        return self._attr_state

    async def async_added_to_hass(self):
        """When entity is added to hass."""
        await super().async_added_to_hass()
        # Set up the initial state
        await self.coordinator.check_for_ota_updates()

    async def async_update(self):
        """Update the sensor."""
        await self.coordinator.check_for_ota_updates()
