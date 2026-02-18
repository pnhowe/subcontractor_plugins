MODULE_NAME = 'redfish'

from subcontractor_plugins.redfish.lib import link_test, set_power, power_state

MODULE_FUNCTIONS = {
                     'link_test': link_test,
                     'set_power': set_power,
                     'power_state': power_state
                   }
