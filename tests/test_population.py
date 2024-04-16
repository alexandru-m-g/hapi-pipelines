from hapi.pipelines.patches.population import _validate_hxl_tag


def test_validate_hxl_tag():
    correct_hxl_tags = [
        "#population+total",
        "#population+f+total",
        "#population+age_5_12+total",
        "#population+age_80_plus+total",
        "#population+f+age_5_12",
        "#population+f+age_80_plus",
    ]
    for hxl_tag in correct_hxl_tags:
        assert _validate_hxl_tag(hxl_tag=hxl_tag)
    incorrect_hxl_tags = [
        "population+total",
        "#population+nonsense",
        "#population+f" "#population+total+f",
        "#population+age_5_12+f" "#population+age_5_12",
        "#population+age_5-12+total",
    ]
    for hxl_tag in incorrect_hxl_tags:
        assert not _validate_hxl_tag(hxl_tag)
