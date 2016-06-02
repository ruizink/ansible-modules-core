#!/usr/bin/python
# coding: utf-8 -*-

# Copyright (c) 2015, Jesse Keating <jlk@derpops.bike>
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.


try:
    import shade
    from shade import meta

    HAS_SHADE = True
except ImportError:
    HAS_SHADE = False

DOCUMENTATION = '''
---
module: os_server_actions
short_description: Perform actions on Compute Instances from OpenStack
extends_documentation_fragment: openstack
version_added: "2.0"
author: "Jesse Keating (@j2sol)"
description:
   - Perform server actions on an existing compute instance from OpenStack.
     This module does not return any data other than changed true/false.
options:
   server:
     description:
        - Name or ID of the instance
     required: true
   wait:
     description:
        - If the module should wait for the instance action to be performed.
     required: false
     default: 'yes'
   timeout:
     description:
        - The amount of time the module should wait for the instance to perform
          the requested action.
     required: false
     default: 180
   action:
     description:
       - Perform the given action. The lock and unlock actions always return
         changed as the servers API does not provide lock status.
     choices: [stop, start, pause, unpause, lock, unlock, suspend, resume, set_medatada, delete_metadata]
     required: true
   meta:
     description:
        - 'A list of key value pairs that should be provided as a metadata to
          the instance or a string containing a list of key-value pairs.
          Eg:  meta: "key1=value1,key2=value2"'
     required: false
     default: None
requirements:
    - "python >= 2.6"
    - "shade"
'''

EXAMPLES = '''
# Pauses a compute instance
- os_server_actions:
       action: pause
       auth:
         auth_url: https://mycloud.openstack.blueboxgrid.com:5001/v2.0
         username: admin
         password: admin
         project_name: admin
       server: vm1
       timeout: 200
'''

_action_map = {'stop': 'SHUTOFF',
               'start': 'ACTIVE',
               'pause': 'PAUSED',
               'unpause': 'ACTIVE',
               'lock': 'ACTIVE', # API doesn't show lock/unlock status
               'unlock': 'ACTIVE',
               'suspend': 'SUSPENDED',
               'resume': 'ACTIVE',}

_admin_actions = ['pause', 'unpause', 'suspend', 'resume', 'lock', 'unlock']

_system_state_actions = ['stop', 'start', 'pause', 'unpause', 'lock', 'unlock', 'suspend', 'resume']

_metadata_actions = ['set_metadata', 'delete_metadata']


def _wait(timeout, cloud, server, action):
    """Wait for the server to reach the desired state for the given action."""

    for count in shade._utils._iterate_timeout(
            timeout,
            "Timeout waiting for server to complete %s" % action):
        try:
            server = cloud.get_server(server.id)
        except Exception:
            continue

        if server.status == _action_map[action]:
            return

        if server.status == 'ERROR':
            module.fail_json(msg="Server reached ERROR state while attempting to %s" % action)


def _system_state_change(action, status):
    """Check if system state would change."""
    if status == _action_map[action]:
        return False
    return True


def _needs_metadata_update(server_metadata={}, metadata={}):
    return len(set(metadata.items()) - set(server_metadata.items())) != 0


def _get_metadate_keys_to_delete(server_metadata_keys=[], metadata_keys=[]):
    return set(server_metadata_keys) & set(metadata_keys)


def _handle_system_state_action(action, module):
    wait = module.params['wait']
    timeout = module.params['timeout']
    try:
        if action in _admin_actions:
            cloud = shade.operator_cloud(**module.params)
        else:
            cloud = shade.openstack_cloud(**module.params)
        server = cloud.get_server(module.params['server'])
        if not server:
            module.fail_json(msg='Could not find server %s' % server)
        status = server.status

        if module.check_mode:
            module.exit_json(changed=_system_state_change(action, status))

        if action == 'stop':
            if not _system_state_change(action, status):
                module.exit_json(changed=False)

            cloud.nova_client.servers.stop(server=server.id)
            if wait:
                _wait(timeout, cloud, server, action)
                module.exit_json(changed=True)

        if action == 'start':
            if not _system_state_change(action, status):
                module.exit_json(changed=False)

            cloud.nova_client.servers.start(server=server.id)
            if wait:
                _wait(timeout, cloud, server, action)
                module.exit_json(changed=True)

        if action == 'pause':
            if not _system_state_change(action, status):
                module.exit_json(changed=False)

            cloud.nova_client.servers.pause(server=server.id)
            if wait:
                _wait(timeout, cloud, server, action)
                module.exit_json(changed=True)

        elif action == 'unpause':
            if not _system_state_change(action, status):
                module.exit_json(changed=False)

            cloud.nova_client.servers.unpause(server=server.id)
            if wait:
                _wait(timeout, cloud, server, action)
            module.exit_json(changed=True)

        elif action == 'lock':
            # lock doesn't set a state, just do it
            cloud.nova_client.servers.lock(server=server.id)
            module.exit_json(changed=True)

        elif action == 'unlock':
            # unlock doesn't set a state, just do it
            cloud.nova_client.servers.unlock(server=server.id)
            module.exit_json(changed=True)

        elif action == 'suspend':
            if not _system_state_change(action, status):
                module.exit_json(changed=False)

            cloud.nova_client.servers.suspend(server=server.id)
            if wait:
                _wait(timeout, cloud, server, action)
            module.exit_json(changed=True)

        elif action == 'resume':
            if not _system_state_change(action, status):
                module.exit_json(changed=False)

            cloud.nova_client.servers.resume(server=server.id)
            if wait:
                _wait(timeout, cloud, server, action)
            module.exit_json(changed=True)

    except shade.OpenStackCloudException as e:
        module.fail_json(msg=str(e), extra_data=e.extra_data)


def _handle_metadata_action(action, module):
    server_name = module.params['server']
    metadata = module.params['meta']
    changed = False

    try:
        cloud_params = dict(module.params)
        cloud = shade.openstack_cloud(**cloud_params)

        server = cloud.get_server(server_name)
        if not server:
            module.fail_json(
                msg='Could not find server %s' % server_name)

        # convert the metadata to dict, in case it was provided as CSV
        if type(metadata) is str:
            metas = {}
            for kv_str in metadata.split(","):
                k, v = kv_str.split("=")
                metas[k] = v
            metadata = metas

        if action == 'set_metadata':
            # check if it needs update
            if _needs_metadata_update(server_metadata=server.metadata, metadata=metadata):
                if module.check_mode:
                    module.exit_json(changed=True)
                else:
                    cloud.set_metadata_server(server_name, metadata)
                    changed = True
        elif action == 'delete_metadata':
            # remove from params the keys that do not exist in the server
            keys_to_delete = _get_metadate_keys_to_delete(server.metadata.keys(), metadata.keys())
            if len(keys_to_delete) > 0:
                if module.check_mode:
                    module.exit_json(changed=True)
                else:
                    cloud.delete_metadata_server(server_name, keys_to_delete)
                    changed = True

        if changed:
            server = cloud.get_server(server_name)

        module.exit_json(
            changed=changed, server_id=server.id, metadata=server.metadata)

    except shade.OpenStackCloudException as e:
        module.fail_json(msg=e.message, extra_data=e.extra_data)


def main():
    argument_spec = openstack_full_argument_spec(
        server=dict(required=True),
        action=dict(required=True, choices=['stop', 'start', 'pause', 'unpause', 'lock', 'unlock', 'suspend', 'resume',
                                            'set_metadata', 'delete_metadata']),
        meta=dict(required=False, default=None),
    )

    module_kwargs = openstack_module_kwargs()
    module = AnsibleModule(argument_spec, supports_check_mode=True, **module_kwargs)

    if not HAS_SHADE:
        module.fail_json(msg='shade is required for this module')

    action = module.params['action']

    if action in _system_state_actions:
        _handle_system_state_action(action, module)

    if action in _metadata_actions:
        _handle_metadata_action(action, module)


# this is magic, see lib/ansible/module_common.py
from ansible.module_utils.basic import *
from ansible.module_utils.openstack import *

if __name__ == '__main__':
    main()
