from hapi_schema.db_admin1 import admin1_view_params
from hapi_schema.db_admin2 import admin2_view_params
from hapi_schema.db_age_range import age_range_view_params
from hapi_schema.db_dataset import dataset_view_params
from hapi_schema.db_gender import gender_view_params
from hapi_schema.db_location import location_view_params
from hapi_schema.db_operational_presence import (
    operational_presence_view_params,
)
from hapi_schema.db_org import org_view_params
from hapi_schema.db_org_type import org_type_view_params
from hapi_schema.db_population import population_view_params
from hapi_schema.db_resource import resource_view_params
from hapi_schema.db_sector import sector_view_params
from hdx.database.views import view

for view_params in [
    admin1_view_params,
    admin2_view_params,
    age_range_view_params,
    dataset_view_params,
    gender_view_params,
    location_view_params,
    operational_presence_view_params,
    org_view_params,
    org_type_view_params,
    population_view_params,
    resource_view_params,
    sector_view_params,
]:
    view(**view_params.__dict__)
