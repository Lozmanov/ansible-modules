#!/usr/bin/python3

import time
from natsort import natsorted
from cm_client.rest import ApiException
import cm_client
from ansible.module_utils.basic import AnsibleModule
import re
from functools import wraps

ANSIBLE_METADATA = {
    "metadata_version": "1.0",
    "supported_by": "community",
    "status": ["preview"],
    "version": "1.1.0"
}


def build_module():
    fields = {
        "cm_login": {"required": True, "type": "str", "no_log": True},
        "cm_password": {"required": True, "type": "str", "no_log": True},
        "cm_host": {"required": True, "type": "str"},
        "cm_port": {"required": False, "type": "str", "default": "7180"},
        "cluster": {"required": True, "type": "str"},
        "api_version": {"required": False, "type": "str", "default": "18"},
        "product": {"required": False, "type": "str"},
        "version": {"required": False, "type": "str", "default": "latest"},
        "state": {
            "default": "infos",
            "choices": ['present', 'distributed', 'activated', 'absent', 'infos'],
            "type": 'str'
        }
    }

    mutually_exclusive = []
    module = AnsibleModule(
        argument_spec=fields,
        mutually_exclusive=mutually_exclusive,
        supports_check_mode=True
    )

    return module


class Parcel:
    def __init__(self, name, version, cluster_name, api_client, module, no_wait=False, **kwargs):
        self.name = name
        self.module = module
        self.cluster_name = cluster_name
        self.api_client = api_client
        self.parcel_api_client_instance = cm_client.ParcelResourceApi(self.api_client)
        self.parcels_api_client_instance = cm_client.ParcelsResourceApi(self.api_client)
        # Getting cluster version to guess a "latest" parcel
        self.api_instance = cm_client.ClouderaManagerResourceApi(self.api_client)
        self.cluster_version = self._get_cluster_version()
        self.version = self._get_versions(version)
        self.no_wait = no_wait
        self.changed = False
        self._update()

    # Try-except decorator to log errors on the cluster management API requests.
    class Decorators(object):
        @classmethod
        def try_cm_api(cls, func, *args):
            @wraps(func)
            def wrapper(*args):
                try:
                    return func(*args)
                except ApiException as e:
                    args[0].module.fail_json(msg=f"Cluster Manager error : {e}")
            return wrapper

    def _update(self):
        self.stage = self._get_stage()
        self.status = self._get_status()

    @Decorators.try_cm_api
    def _get_status(self):
        return self.parcel_api_client_instance.read_parcel(self.cluster_name, self.name, self.version).state

    @Decorators.try_cm_api
    def _get_stage(self):
        return self.parcel_api_client_instance.read_parcel(self.cluster_name, self.name, self.version).stage.lower()

    @Decorators.try_cm_api
    def _get_cluster_version(self):
        return self.api_instance.get_version().version

    @Decorators.try_cm_api
    def _get_parcels_list(self):
        return self.parcels_api_client_instance.read_parcels(self.cluster_name).items

    def _get_versions(self, version):
        if version == "latest":
            versions = []
            for parcel in self._get_parcels_list():
                guess = re.compile(f'^.*cdh{self.cluster_version[0]}$')  # i.e. "^.*cdh6$"
                if (parcel.product == self.name):
                    # Kinda guessing the version of Fusion Client. I have doubts about it.
                    if guess.match(parcel.version.lower()) or ("cdh" not in parcel.version):
                        versions.append(parcel.version)
            if len(versions) > 0:
                version = natsorted(versions)[-1]
        return version

    def _check_transition(self):
        self._update()
        if self.no_wait:
            return True
        trans_states = ["downloading", "distributing", "undistributing", "activating"]
        while (
            (self.status.total_count > 0)
            and (self.status.total_count != self.status.count)
            or (self.stage in trans_states)
        ):
            time.sleep(1)
            self._update()

    @Decorators.try_cm_api
    def _download(self):
        self.parcel_api_client_instance.start_download_command(self.cluster_name, self.name, self.version)

    @Decorators.try_cm_api
    def _distribute(self):
        self.parcel_api_client_instance.start_distribution_command(self.cluster_name, self.name, self.version)

    @Decorators.try_cm_api
    def _activate(self):
        self.parcel_api_client_instance.activate_command(self.cluster_name, self.name, self.version)

    @Decorators.try_cm_api
    def _deactivate(self):
        self.parcel_api_client_instance.deactivate_command(self.cluster_name, self.name, self.version)

    @Decorators.try_cm_api
    def _remove_distribution(self):
        self.parcel_api_client_instance.start_removal_of_distribution_command(
            self.cluster_name, self.name, self.version
        )

    @Decorators.try_cm_api
    def _remove_downloaded(self):
        self.parcel_api_client_instance.remove_download_command(self.cluster_name, self.name, self.version)

    def downloaded(self):
        if self.stage != "downloaded":
            if self.stage == "activated":
                self.deactivate()
            if self.stage == "distributed":
                self.undistribute()
            self._download()
            self._check_transition()
            self.changed = True

    def distributed(self):
        if self.stage != "distributed":
            if self.stage != "downloaded":
                if self.stage == "available_remotely":
                    self.downloaded()
                elif self.stage == "activated":
                    self.deactivate()
            self._distribute()
            self._check_transition()
            self.changed = True

    def activated(self):
        if self.stage != "activated":
            if self.stage != "distributed":
                self.distributed()
            self._activate()
            self._check_transition()
            self.changed = True

    def deactivate(self):
        self._deactivate()
        self._check_transition()

    def undistribute(self):
        self._remove_distribution()
        self._check_transition()

    def available_remotely(self):
        if self.stage != "available_remotely":
            if self.stage != "downloaded":
                if self.stage == "activated":
                    self.deactivate()
                if self.stage == "distributed":
                    self.undistribute()
            self._remove_downloaded()
            self._check_transition()
            self.changed = True

    def meta(self):
        meta = {
            "product": self.name,
            "version": self.version,
            "stage": self.stage
        }
        return meta

    def __repr__(self):
        return f'Parcel(name="{self.name}", version="{self.version}", cluster_name="{self.cluster_name}",\
            api_client={self.api_client}, stage="{self.stage}", status={self.status})'

    def __str__(self):
        return f"name: {self.name}, version: {self.version}, state: {self.stage}"


def main():
    module = build_module()
    choice_map = {
        'present': 'downloaded',
        'distributed': 'distributed',
        'activated': 'activated',
        'absent': 'available_remotely'
    }
    params = module.params

    api_url = f"http://{params['cm_host']}:{params['cm_port']}/api/v{params['api_version']}"
    cm_client.configuration.username = params['cm_login']
    cm_client.configuration.password = params['cm_password']
    cm_client.configuration.host = api_url
    api_client = cm_client.ApiClient()

    # Getting info at first. Info can be without any product and version, just about all available parcels.
    if params["state"] == "infos":
        api_client_instance = cm_client.ParcelsResourceApi(api_client)
        try:
            parcels = []
            for parcel in api_client_instance.read_parcels(params["cluster"]).items:
                if params["product"] is not None:
                    # Info about a specific product?
                    if params["product"] != parcel.product:
                        continue
                    # Info about a specific version? "Latest" will not work here.
                    # TODO: Regex and "latest" detection
                    supposed_parcel = Parcel(
                        params["product"],
                        params["version"],
                        params["cluster"],
                        api_client,
                        module
                    )
                    if supposed_parcel.version != parcel.version:
                        continue
                parcels.append(
                    Parcel(
                        name=parcel.product,
                        version=parcel.version,
                        cluster_name=parcel.cluster_ref.cluster_name,
                        api_client=api_client,
                        module=module
                    ).meta()
                )
            module.exit_json(changed=False, msg="Parcels informations gathered", meta=parcels)
        except ApiException as e:
            module.fail_json(msg=f"Cluster error : {e}")
    else:
        if params["product"] is not None:
            parcel = Parcel(params["product"], params["version"], params["cluster"], api_client, module)
            try:
                getattr(parcel, choice_map.get(params["state"]))()
            except ApiException as e:
                module.fail_json(msg=f"Cluster error : {e}")
            module.exit_json(changed=parcel.changed, msg=f"{parcel.name} is {parcel.stage}", meta=parcel.meta())
        else:
            module.fail_json(changed=False,
                             msg="No valid parameters combination was used: \"product\" is not set, exiting", meta=[])


if __name__ == "__main__":
    main()
