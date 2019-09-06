import pytest
from gnmi_utils import (
    split_path,
    parse_step,
    split_predicates,
    simple_gnmi_string_parser,
    gnmi_path_to_string,
    gnmi_to_string_and_keys,
)


class TestGNMIPath:
    @pytest.mark.parametrize(
        "path, expected",
        [
            (
                "/interfaces/interface[name=Ethernet/1/2/3]/state/counters",
                ["interfaces", "interface[name=Ethernet/1/2/3]", "state", "counters"],
            ),
            ("interfaces/interface/", ["interfaces", "interface"]),
            (
                r"interfaces/interface[name=interface\]]/",
                ["interfaces", r"interface[name=interface\]]"],
            ),
            (
                r"interfaces/interface[name=ethernet\\3\]]/",
                ["interfaces", r"interface[name=ethernet\\3\]]"],
            ),
            ("interfaces/...//", ["interfaces", "...", ""]),
            ("a/b/c", ["a", "b", "c"]),
        ],
    )
    def test_split_path(self, path, expected):
        obtained = list(split_path(path))
        assert obtained == expected

    @pytest.mark.parametrize(
        "step, expected",
        [
            (
                "name[predicate1=one][predicate2=two]",
                ["name", "predicate1=one]", "predicate2=two]"],
            ),
            (
                r"name[predicate1=[one][predicate2=\\two][\]]",
                ["name", "predicate1=[one]", r"predicate2=\\two]", r"\]]"],
            ),
            ("a", ["a"]),
            (
                r"[predicate1=[one][predicate2=\\two][\]]",
                ["", "predicate1=[one]", r"predicate2=\\two]", r"\]]"],
            ),
        ],
    )
    def test_split_predicate(self, step, expected):
        obtained = list(split_predicates(step))
        assert obtained == expected

    @pytest.mark.parametrize(
        "step, name, constraints",
        [
            ("interface[name=Ethernet/1/2/3]", "interface", {"name": "Ethernet/1/2/3"}),
            ("...", "...", {}),
            ("simple[test=34][op=34]", "simple", {"test": "34", "op": "34"}),
            (r"foo[name=[\\\]]", "foo", {"name": r"[\]"}),
            ("foo[name=[]", "foo", {"name": "["}),
            (r"foo[name=\]]", "foo", {"name": "]"}),
            (r"foo[name=\\]", "foo", {"name": "\\"}),
            (
                r"namespace:protocol[identifier=ISIS][name=65497][other=\\]",
                "namespace:protocol",
                {"identifier": "ISIS", "name": "65497", "other": "\\"},
            ),
        ],
    )
    def test_step_decode(self, step, name, constraints):
        oname, oconstraints = parse_step(step)
        assert oname == name
        assert oconstraints == constraints

    # TODO: Do a proper test with the output, here I am just making sure they compile
    @pytest.mark.parametrize(
        "path",
        [
            "/a/b/c",
            "a/b/c",
            "/interfaces/interface[name=Ethernet/1/2/3]/state",
            "/interfaces/interface[name=Ethernet/1/2/3]/state/counters",
            "/network-instances/network-instance[name=DEFAULT]/protocols/protocol[identifier=ISIS][name=65497]",
            "/foo[name=\]]",
            "/foo[name=[]",
            r"/foo[name=[\\\]]",  # I am having doubts about this one...
        ],
    )
    def test_paths(self, path):
        gnmi_path = simple_gnmi_string_parser(path)
        print(path, gnmi_path)
        remove_root = lambda x: x[1:] if x[0] == "/" else x
        re_formated_path = gnmi_path_to_string(gnmi_path)
        assert remove_root(path) == re_formated_path
        simple_path, keys = gnmi_to_string_and_keys(gnmi_path)
