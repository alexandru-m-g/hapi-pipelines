from datetime import datetime
from typing import Dict

from hdx.location.adminlevel import AdminLevel

from hapi.pipelines.patches.admins import Admins
from hapi.pipelines.patches.locations import Locations


class Pipelines:
    def __init__(
        self,
        configuration: Dict,
        today: datetime,
        use_live: bool = True,
    ):
        self.configuration = configuration
        self.today = (today,)
        self.locations = Locations(
            configuration=configuration, today=today, use_live=use_live
        )
        countries = configuration["HAPI_countries"]
        libhxl_dataset = AdminLevel.get_libhxl_dataset().cache()
        self.admins = Admins(
            configuration=configuration,
            libhxl_dataset=libhxl_dataset,
            locations=self.locations,
            today=today,
        )
        self.adminone = AdminLevel(admin_level=1)
        self.admintwo = AdminLevel(admin_level=2)
        self.adminone.setup_from_libhxl_dataset(libhxl_dataset, countries)
        self.adminone.load_pcode_formats()
        self.admintwo.setup_from_libhxl_dataset(libhxl_dataset, countries)
        self.admintwo.load_pcode_formats()
        self.admintwo.set_parent_admins_from_adminlevels([self.adminone])

        # self.population_status = PopulationStatus(
        #    population_status_descriptions=configuration[
        #        "population_status_descriptions"
        #    ],
        # )
        # self.population_group = PopulationGroup(
        #    population_group_descriptions=configuration[
        #        "population_group_descriptions"
        #    ],
        # )
        # self.org_type = OrgType(
        #    datasetinfo=configuration["org_type"],
        #    org_type_map=configuration["org_type_map"],
        # )
        # self.ipc_phase = IpcPhase(
        #    ipc_phase_names=configuration["ipc_phase_names"],
        #    ipc_phase_descriptions=configuration["ipc_phase_descriptions"],
        # )
        # self.ipc_type = IpcType(
        #    ipc_type_descriptions=configuration["ipc_type_descriptions"],
        # )
        # self.gender = Gender(
        #    gender_descriptions=configuration["gender_descriptions"],
        # )

        # Sources.set_default_source_date_format("%Y-%m-%d")

    def output(self):
        self.locations.generate_hapi_patch()
        self.admins.generate_hapi_patch()


#        self.population_status.generate_hapi_patch()
#        self.population_group.generate_hapi_patch()
#        self.org_type.generate_hapi_patch()
#        self.ipc_phase.generate_hapi_patch()
#        self.ipc_type.generate_hapi_patch()
#        self.gender.generate_hapi_patch()
#
