MODULE_NAME = 'proxmox'

from subcontractor_plugins.proxmox.lib import create, destroy, set_power, power_state, get_interface_map, node_list

MODULE_FUNCTIONS = {
                     'create': create,
                     'destroy': destroy,
                     'set_power': set_power,
                     'power_state': power_state,
                     'get_interface_map': get_interface_map,
                     'node_list': node_list
                   }
