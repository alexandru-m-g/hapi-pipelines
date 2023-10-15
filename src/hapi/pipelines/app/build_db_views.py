from hapi_schema.db_admin1 import view_params_admin1
from hapi_schema.db_admin2 import view_params_admin2
from hapi_schema.db_age_range import view_params_age_range
from hapi_schema.db_dataset import view_params_dataset
from hapi_schema.db_gender import view_params_gender
from hapi_schema.db_location import view_params_location
from hapi_schema.db_operational_presence import (
    view_params_operational_presence,
)
from hapi_schema.db_org import view_params_org
from hapi_schema.db_org_type import view_params_org_type
from hapi_schema.db_population import view_params_population
from hapi_schema.db_resource import view_params_resource
from hapi_schema.db_sector import view_params_sector
from hdx.database.views import view

for view_params in [
    view_params_admin1,
    view_params_admin2,
    view_params_age_range,
    view_params_dataset,
    view_params_gender,
    view_params_location,
    view_params_operational_presence,
    view_params_org,
    view_params_org_type,
    view_params_population,
    view_params_resource,
    view_params_sector,
]:
    view(**view_params.__dict__)
