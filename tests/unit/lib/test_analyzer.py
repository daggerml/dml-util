from dml_util.lib.analyzer import build_class_graph


def dec(fn):
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapper


class BasicAccess:
    const = 3

    def m2(self, x):
        return x

    def return_value(self):
        return 42

    def define_first(self, x):
        self.foo = 12  # should not count as dependency
        return self.foo + x

    def use_internal(self, x):
        return self.m2(x) + self.const

    def define_fn_with_deps_inside(self, x):
        def inner_fn(y):
            return self.m2(y)
        return inner_fn(x) + self.const

    def define_fn_with_deps_inside_no_use(self, x):
        def inner_fn(y):
            return self.m2(y)
        return self.const + x

    def recursive_use_internal(self, x):
        return self.use_internal(x - 1)

    @dec
    def return_value_decorated(self):
        return 42

    @dec
    def define_first_decorated(self, x):
        self.foo = 12
        return self.foo + x

    @dec
    def use_internal_decorated(self, x):
        return self.m2(x) + self.const


class TestBasicAccess:
    def test_m2(self):
        graph = build_class_graph(BasicAccess)
        assert graph["m2"] == {"internal": []}

    def test_return_value(self):
        graph = build_class_graph(BasicAccess)
        assert graph["return_value"] == {"internal": []}

    def test_define_first(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_first"] == {"internal": []}

    def test_use_internal(self):
        graph = build_class_graph(BasicAccess)
        assert graph["use_internal"] == {"internal": ["const", "m2"]}

    def test_define_fn_with_deps_inside(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_fn_with_deps_inside"] == {"internal": ["const", "m2"]}

    def test_define_fn_with_deps_inside_no_use(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_fn_with_deps_inside_no_use"] == {"internal": ["const", "m2"]}

    def test_recursive_use_internal(self):
        graph = build_class_graph(BasicAccess)
        assert graph["recursive_use_internal"] == {"internal": ["use_internal"]}

    def test_return_value_decorated(self):
        graph = build_class_graph(BasicAccess)
        assert graph["return_value_decorated"] == {"internal": []}

    def test_define_first_decorated(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_first_decorated"] == {"internal": []}

    def test_use_internal_decorated(self):
        graph = build_class_graph(BasicAccess)
        assert graph["use_internal_decorated"] == {"internal": ["const", "m2"]}
