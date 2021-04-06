# ansible-modules

Plugins have a modular structure. Common class is in `plugins/module_utils`. Plugins themselves are in `plugins/modules`.
Ansible config points to it. Paths can be changed if plugins are checkouts into a separate folder.
Please note that there are python dependencies: `requirements.txt`
`ansible.cfg`:
```
[defaults]
library = ./plugins/modules
module_utils = ./plugins/module_utils
```

Example playbook:
```
---
- name: Example to activate the parcel
  hosts: localhost
  become: false
  tasks:
    - name: Add parcel repo
      cloudera_config_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        config_parameter: REMOTE_PARCEL_REPO_URLS
        config_value: "http://fusion-repo.wandisco.com/parcels/"
        action: append

    - name: Activate parcels
      cloudera_parcel_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        product: "{{ item }}"
        version: latest
        state: activated
      loop:
        - FUSION
        - FUSION_CLIENT

    - name: Create role zookeeper server on the nmish01
      cloudera_role_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        services: zookeeper1
        action: roles-add
        roles: '[{"host":"nmish01-vm1.bdfrem.wandisco.com", "type":"SERVER"}]'

    - name: Start role zookeeper server on the nmish01
      cloudera_role_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        services: zookeeper1
        action: roles-start
        roles: '[{"host":"nmish01-vm1.bdfrem.wandisco.com", "type":"SERVER"}]'

    - name: Start role zookeeper server on the nmish01
      cloudera_role_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        services: zookeeper1
        action: roles-delete
        roles: '[{"host":"nmish01-vm1.bdfrem.wandisco.com", "type":"SERVER"}]'

    - name: Set role config
      cloudera_role_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        services: hdfs1
        action: roles-set-config
        config_view: summary
        roles: '[{
          "type":"NAMENODE"
        }]'
        roles_configs: '{
          "fs_checkpoint_period": "3600"
        }'

    - name: Restart stale services
      cloudera_state_manager:
        cm_login: admin
        cm_password:  admin
        cm_host: dcher01-vm0
        cm_proto: http
        cm_port: 7180
        action: restart
        only_stale_services: true

    - name: Change some config and apply it (without restarting any services)
      cloudera_config_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        config_parameters: '{
          "COMMAND_EVICTION_AGE_HOURS": "17200",
          "TAGS_LIMIT": "100001"
        }'
        action: set

    - name: Rolling restart stale services (requires HA, Enterprise license, and takes longer than just restart)
      cloudera_state_manager:
        cm_login: admin
        cm_password:  admin
        cm_host: dcher01-vm0
        cm_proto: http
        cm_port: 7180
        action: rolling-restart
        only_stale_services: true
        rolling_restart_roles_type: all_roles

    - name: Deploy yarn1 client config
      cloudera_role_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        services: yarn1
        action: deploy-client-config

    # Warning!!! It's big.
    - name: Get cluster info (cluster, services, hosts, roles, config)
      cloudera_state_manager:
        cm_login: admin
        cm_password: admin
        cm_host: dcher01-vm0
        cluster: DCHER-01
        action: info
        config_view: full
      register: message_to_print

    - name: Print cluster info
      debug:
        msg: "{{ message_to_print }}"
```
