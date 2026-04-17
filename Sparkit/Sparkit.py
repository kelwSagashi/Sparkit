"""

sparkit Python mini-SDK


Funcionalidades:

 - procedural: @Input / @Output / @Run decoram a função; sparkit.run(main) roda

 - OO: @Node (ou @Node()) marca a classe como node, podemos não herdar de Node

 - python script.py --schema  -> imprime schema json (inputs/outputs) com detalhamento completo

 - execução normal: espera JSON no stdin quando não interativo

 - validações: evita mistura procedural + OO, erros claros
"""
from __future__ import annotations

import sys, json, inspect, traceback, io

from contextlib import redirect_stdout, redirect_stderr

from typing import get_type_hints, get_origin, get_args, Any, Dict, List, Union, Optional
import ast
import textwrap


# | 'trigger'

# | 'error'

# | 'stdout'

# | 'stderr'

# | 'success'

# | 'arg'

# | 'string'

# | 'number'

# | 'boolean'

# | 'object'

# | 'array'

# | 'json'

# | 'unknown'

TYPE_NAME_MAP = {

    str: "string",

    int: "number",

    float: "number",

    bool: "boolean",

    list: "array",

    dict: "json",

    object: "object",

}


def type_to_str(tp: Any) -> str:

    """Mapeia qualquer tipo Python para um nome simples do dicionário TYPE_NAME_MAP."""

    try:

        if tp in TYPE_NAME_MAP:

            return TYPE_NAME_MAP[tp]


        origin = get_origin(tp)

        args = get_args(tp)


        if origin in (list, List):

            return "array"


        if origin in (dict, Dict):

            return "json"


        if origin is Union:

            for a in args:

                mapped = type_to_str(a)

                if mapped != "unknown":
                    return mapped

            return "unknown"


        if hasattr(tp, "__name__"):

            return TYPE_NAME_MAP.get(tp, "unknown")


        return "unknown"


    except Exception:

        return "unknown"



# ---------- registries / small classes ----------


# Schema estático do campo `stderr` — sempre o mesmo formato

_STDERR_SCHEMA = {
    "name": "stderr",

    "type": "object",

    "description": "Present when an unhandled exception occurs during execution.",

    "fields": [

        {"name": "type",      "type": "string", "description": "Exception class name (e.g. ValueError, RuntimeError)."},

        {"name": "message",   "type": "string", "description": "Human-readable error message."},

        {"name": "traceback", "type": "string", "description": "Full Python traceback as a string."},

    ],

    "nullable": True,

}


class OutputRegistry:
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.definitions: Dict[str, Dict[str, Any]] = {
            'stdout': {'name': 'stdout', 'type': Any, 'description': None},
            'stderr': {'name': 'stderr', 'type': Dict[str, str], 'description': None},
        }

    def add(self, name: str, type_: Any = Any, description: str = None):
        """Define uma saída com nome, tipo e descrição opcional."""
        self.definitions[name] = {"name": name, "type": type_, "description": description}

    def set_data(self, name: str, data: Any):
        if name not in self.definitions:
            raise ValueError(
                f"Output '{name}' not declared. Use @Output('{name}') or "
                f"sparkit.outputs.add('{name}') before setting."
            )
        self.data[name] = data

    def add_data(self, name: str, data: Any):
        self.set_data(name, data)


class InputRegistry:
    def __init__(self):
        self.fields: Dict[str, Dict[str, Any]] = {}

    def add(self, name: str, required: bool = False, type_: Any = str, description: str = None):
        self.fields[name] = {"name": name, "required": required, "type": type_, "description": description}

    def clear(self):
        self.fields.clear()


def ensure_meta(obj):
    if not hasattr(obj, "__sparkit_meta__"):
        obj.__sparkit_meta__ = {"inputs": {}, "outputs": {}, "run_method": None}
    return obj.__sparkit_meta__


def Input(name: str = None, required: bool = False, type: Any = Any, description: str = None):

    def deco(target):

        if inspect.isfunction(target):

            meta = getattr(target, "__sparkit_proc_meta__", None)

            if meta is None:

                target.__sparkit_proc_meta__ = {"inputs": {}, "outputs": {}, "run": False}

                meta = target.__sparkit_proc_meta__

            key = name or (target.__name__)

            meta["inputs"][key] = {"required": required, "type": type, "description": description}

            return target

        if inspect.isclass(target):

            meta = ensure_meta(target)

            key = name or None

            if key is None:

                raise ValueError("@Input used on class must specify 'name' parameter")

            meta["inputs"][key] = {"required": required, "type": type, "description": description}

            return target

        return target
    return deco



def Output(name: str, type: Any = Any, description: str = None):
    """

    Register an output with a name, optional type and optional description.
    """

    def deco(target):

        meta_key = "__sparkit_proc_meta__" if inspect.isfunction(target) else "__sparkit_meta__"


        if inspect.isfunction(target) or inspect.isclass(target):

            if not hasattr(target, meta_key):

                if inspect.isfunction(target):

                    setattr(target, meta_key, {"inputs": {}, "outputs": {}, "run": False})
                else:

                    ensure_meta(target)


            meta = getattr(target, meta_key)

            meta["outputs"][name] = {"type": type, "description": description}

            return target

        return target
    return deco



def Run(fn):
    if not inspect.isfunction(fn):
        return fn

    meta = getattr(fn, "__sparkit_proc_meta__", None)

    if meta is None:

        fn.__sparkit_proc_meta__ = {"inputs": {}, "outputs": {}, "run": True}
    else:

        meta["run"] = True

    fn.__sparkit_is_run_method__ = True
    return fn



def MainOut(fn):
    if not inspect.isfunction(fn):

        raise TypeError("@MainOut can only decorate methods.")

    fn.__sparkit_is_main_out__ = True
    return fn



def Out(name: str):
    if not isinstance(name, str) or not name:

        raise TypeError("@Out decorator requires a non-empty string name.")
    def decorator(fn):
        if not inspect.isfunction(fn):

            raise TypeError("@Out can only decorate methods.")

        fn.__sparkit_output_name__ = name
        return fn
    return decorator



def NodeDecorator(arg=None):

    def _decorate(clazz):

        meta = ensure_meta(clazz)

        clazz.__sparkit_is_node__ = True

        run_method_found = None

        for name, member in inspect.getmembers(clazz, predicate=inspect.isfunction):

            if hasattr(member, '__sparkit_is_run_method__'):

                if run_method_found:

                    raise TypeError(f"Multiple @Run methods in {clazz.__name__}.")

                run_method_found = name

        if run_method_found:

            meta['run_method'] = run_method_found

        if "__init__" not in clazz.__dict__ or clazz.__init__ is object.__init__:

            def __init__(self, **kwargs):

                hints = {

                    k: v for k, v in get_type_hints(clazz, include_extras=True).items()

                    if k not in ("outputs", "outputs_def") and not k.startswith("_")

                }

                defaults = {

                    k: v for k, v in clazz.__dict__.items()

                    if not k.startswith("_") and not callable(v) and k not in ("outputs", "outputs_def")

                }

                inputs_meta = meta.get("inputs", {})

                for name, typ in hints.items():

                    if name in kwargs:

                        setattr(self, name, kwargs[name])
                    elif name in defaults:

                        setattr(self, name, defaults[name])

                    elif name in inputs_meta and inputs_meta[name]["required"] is False:

                        setattr(self, name, None)

                    elif (name in inputs_meta and inputs_meta[name].get("required")) or (name not in defaults):

                        raise ValueError(f"Missing required input: {name}")
                for name, m in inputs_meta.items():

                    if name in hints:
                        continue

                    if name in kwargs:

                        setattr(self, name, kwargs[name])
                    elif "default" in m:

                        setattr(self, name, m.get("default"))

                    elif m.get("required"):

                        raise ValueError(f"Missing required input: {name}")
                    else:

                        setattr(self, name, None)


                self.outputs = OutputRegistry()

                for name, definition in meta.get("outputs", {}).items():

                    self.outputs.add(name, definition.get("type", Any), definition.get("description"))

                for o_name in getattr(clazz, "outputs_def", []):
                    if isinstance(o_name, str):
                        self.outputs.add(o_name)
                    else:

                        self.outputs.add(o_name, definition.get("type", Any), definition.get("description"))


            setattr(clazz, "__init__", __init__)
        else:

            orig_init = clazz.__init__

            def wrapped_init(self, *a, **kw):

                orig_init(self, *a, **kw)

                if not hasattr(self, "outputs"):

                    self.outputs = OutputRegistry()

                    for name, definition in meta.get("outputs", {}).items():

                        self.outputs.add(name, definition.get("type", Any), definition.get("description"))

                    for o_name in getattr(clazz, "outputs_def", []):
                        self.outputs.add(o_name)

            setattr(clazz, "__init__", wrapped_init)

        return clazz


    if inspect.isclass(arg):

        return _decorate(arg)
    return _decorate



Node = NodeDecorator



# ---------- base Node class (optional to inherit) ----------

class NodeBase:
    """

    Optional base class. Prefer using the @Node decorator on a plain class.
    """

    outputs: OutputRegistry = None

    outputs_def: Dict[str, Dict[str, Any]] = {}


    def __init__(self, **kwargs):

        hints = {

            k: v for k, v in get_type_hints(self.__class__).items()

            if not k.startswith("_") and k not in ("outputs", "outputs_def")

        }

        defaults = {

            k: v for k, v in self.__class__.__dict__.items()

            if not k.startswith("_") and not callable(v) and k not in ("outputs", "outputs_def")

        }


        for name, type_ in hints.items():

            if name in kwargs:

                setattr(self, name, kwargs[name])
            elif name in defaults:

                setattr(self, name, defaults[name])
            else:

                raise ValueError(f"Missing required input: {name}")


        self.outputs = OutputRegistry()


        class_outputs = getattr(self.__class__, "outputs_def", {})
        for name, definition in class_outputs.items():

            self.outputs.add(name, definition.get('type', Any), definition.get('description'))


        meta = getattr(self.__class__, "__sparkit_meta__", {})

        decorator_outputs = meta.get("outputs", {})
        for name, definition in decorator_outputs.items():

            self.outputs.add(name, definition.get("type", Any), definition.get("description"))

    def run(self):

        raise NotImplementedError("override run() or use @Run on a method")



# ---------- runtime ----------
class SparkitRuntime:

    _oo_registered = False

    _procedural_registered = False

    def __init__(self):

        self.main_output: Any = None

        self.inputs = InputRegistry()

        self.outputs = OutputRegistry()


    def set_stdout(self, data: Any):

        self.main_output = data


    def _convert_type(self, value: str, type_str: str):

        try:

            if type_str == "number":

                return int(value) if value.isdigit() else float(value)

            if type_str == "boolean":

                return value.lower() in ("true", "1", "yes")

            if type_str == "array":

                return json.loads(value)

            if type_str == "json":

                return json.loads(value)

            return value

        except Exception:

            raise ValueError(f"Failed to convert '{value}' to {type_str}")


    def _print_help(self, schema):

        print("\nsparkit Script SDK\n")


        print("Inputs:")

        for inp in schema["inputs"]:

            req = "required" if inp.get("required") else "optional"

            desc = f" — {inp['description']}" if inp.get("description") else ""

            print(f"  --{inp['name']} ({inp['type']}, {req}){desc}")


        print("\nOther options:")

        print("  --input '<json>'        JSON inline input")

        print("  --input-file <file>    Load JSON from file")

        print("  --schema               Show schema")

        print("  --help                 Show this help\n")


        print("Outputs:")

        for out in schema["outputs"]:

            desc = f" — {out['description']}" if out.get("description") else ""

            nullable = " (nullable)" if out.get("nullable") else ""

            print(f"  {out['name']} ({out['type']}){nullable}{desc}")
            if "fields" in out:

                for field in out["fields"]:

                    print(f"    .{field['name']} ({field['type']}): {field.get('description','')}")

        print()


    def _parse_cli_args(self, schema):

        args = sys.argv[1:]


        if "--input" in args:

            idx = args.index("--input")

            try:

                return json.loads(args[idx + 1])

            except Exception as e:

                raise ValueError(f"Invalid JSON in --input: {e}")


        if "--input-file" in args:

            idx = args.index("--input-file")

            try:

                with open(args[idx + 1], "r", encoding="utf-8") as f:

                    return json.load(f)

            except Exception as e:

                raise ValueError(f"Error reading --input-file: {e}")


        cli_inputs = {}

        skip = False

        schema_map = {i["name"]: i for i in schema["inputs"]}


        for i, arg in enumerate(args):

            if skip:

                skip = False
                continue


            if not arg.startswith("--"):
                continue


            key = arg[2:]


            if key in ("help", "schema", "input", "input-file"):
                continue


            if key not in schema_map:

                raise ValueError(f"Unknown argument: --{key}")


            try:

                raw_value = args[i + 1]

                skip = True

            except IndexError:

                raise ValueError(f"Missing value for --{key}")


            type_str = schema_map[key]["type"]

            value = self._convert_type(raw_value, type_str)

            cli_inputs[key] = value


        for inp in schema["inputs"]:

            if inp.get("required") and inp["name"] not in cli_inputs:

                raise ValueError(f"Missing required input: {inp['name']}")

        return cli_inputs

    def _read_inputs(self):

        if hasattr(self, "_cli_override_inputs"):

            return self._cli_override_inputs

        args = sys.argv[1:]


        if "--input" in args:

            idx = args.index("--input")

            try:

                raw = args[idx + 1]

                return json.loads(raw)

            except Exception as e:

                raise ValueError(f"Invalid JSON passed to --input: {e}")


        if "--input-file" in args:

            idx = args.index("--input-file")

            try:

                path = args[idx + 1]

                with open(path, "r", encoding="utf-8") as f:

                    return json.load(f)

            except Exception as e:

                raise ValueError(f"Error reading --input-file: {e}")


        cli_inputs = {}

        skip_next = False


        for i, arg in enumerate(args):

            if skip_next:

                skip_next = False
                continue


            if arg.startswith("--") and arg not in ("--schema", "--input", "--input-file"):

                key = arg[2:]

                try:

                    value = args[i + 1]

                    skip_next = True


                    if value.isdigit():

                        value = int(value)

                    elif value.lower() in ("true", "false"):

                        value = value.lower() == "true"


                    cli_inputs[key] = value


                except IndexError:

                    raise ValueError(f"Missing value for argument {arg}")

        if cli_inputs:
            return cli_inputs

    def _infer_fields_from_callable(self, fn: callable) -> List[Dict[str, Any]]:
        """
        Best-effort static analysis of a function/method to find Return statements
        that return dict literals and extract their keys and a simple inferred type.
        Returns a list of field descriptors: {name, type, nullable?}
        """
        fields: Dict[str, Dict[str, Any]] = {}
        try:
            src = inspect.getsource(fn)
        except (OSError, TypeError):
            return []

        # dedent source (methods inside classes are indented) so ast.parse can handle it
        try:
            src = textwrap.dedent(src)
        except Exception:
            pass

        try:
            tree = ast.parse(src)
        except Exception:
            return []

        def infer_type(node: ast.AST) -> tuple[str, bool]:
            # returns (type_name, nullable)
            if isinstance(node, ast.Constant):
                val = node.value
                if val is None:
                    return ("unknown", True)
                if isinstance(val, bool):
                    return ("boolean", False)
                if isinstance(val, (int, float)):
                    return ("number", False)
                if isinstance(val, str):
                    return ("string", False)
                return ("unknown", False)
            if isinstance(node, (ast.Num,)):
                return ("number", False)
            if isinstance(node, (ast.Str,)):
                return ("string", False)
            if isinstance(node, ast.NameConstant):
                if node.value is None:
                    return ("unknown", True)
            # Calls/Attributes/Names/etc: unknown (but often string)
            return ("unknown", False)

        class ReturnVisitor(ast.NodeVisitor):
            def visit_Return(self, node: ast.Return):
                val = node.value
                if isinstance(val, ast.Dict):
                    keys = val.keys
                    values = val.values
                    for k, v in zip(keys, values):
                        if isinstance(k, ast.Constant) and isinstance(k.value, str):
                            key = k.value
                        elif isinstance(k, ast.Str):
                            key = k.s
                        else:
                            continue
                        tname, nullable = infer_type(v)
                        prev = fields.get(key)
                        if prev is None:
                            fields[key] = {"type": tname, "nullable": nullable}
                        else:
                            # merge: if types differ, fallback to unknown; keep nullable if any True
                            if prev["type"] != tname:
                                prev["type"] = "unknown"
                            prev["nullable"] = prev.get("nullable", False) or nullable

        try:
            ReturnVisitor().visit(tree)
        except Exception:
            return []

        out = []
        for k, v in fields.items():
            fd = {"name": k, "type": v.get("type", "unknown")}
            if v.get("nullable"):
                fd["nullable"] = True
            out.append(fd)
        return out


        if sys.stdin.isatty():

            return {}


        raw = sys.stdin.read().strip()

        if not raw:

            return {}


        try:

            return json.loads(raw)

        except Exception as e:

            raise ValueError(f"Invalid JSON from stdin: {e}")


    # ------------------------------------------------------------------

    # Schema de outputs — inclui detalhamento de stdout, stderr e custom

    # ------------------------------------------------------------------

    def _format_outputs_for_schema(self, output_defs: Dict[str, Dict[str, Any]]) -> List[Dict]:

        result = []
        for name, d in output_defs.items():

            if name == "stderr":

                result.append(_STDERR_SCHEMA.copy())
                continue


            t = d.get("type", Any)
            entry: Dict[str, Any] = {
                "name": name,
                "type": type_to_str(t),
            }
            


            # Descrição opcional

            if d.get("description"):

                entry["description"] = d["description"]


            # Para stdout: indica que é a saída principal

            if name == "stdout":

                entry.setdefault("description", "Main output of the node.")

                entry["nullable"] = True

            # If the output definition already included inferred 'fields' (from static analysis),
            # copy them into the schema entry and mark the type as object.
            if d.get("fields"):
                try:
                    entry["fields"] = d.get("fields")
                    entry["type"] = "object"
                except Exception:
                    pass

            # If the declared type is a TypedDict-like class or a class with __annotations__,
            # try to expose field-level info in the schema.
            try:
                # handle typing.TypedDict or simple Pydantic-style dict-like annotations
                if isinstance(t, type) and hasattr(t, '__annotations__') and t is not dict:
                    fields = []
                    for fname, ftype in get_type_hints(t, include_extras=True).items():
                        fields.append({"name": fname, "type": type_to_str(ftype)})
                    if fields:
                        entry["fields"] = fields
            except Exception:
                pass

            # If we inferred structure fields for this output, present it as an object
            if entry.get("fields"):
                entry["type"] = "object"

            result.append(entry)

        return result


    def _get_combined_proc_config(self, fn: callable) -> dict:

        meta = getattr(fn, "__sparkit_proc_meta__", {"inputs": {}, "outputs": {}})


        combined_inputs = self.inputs.fields.copy()

        combined_inputs.update(meta.get("inputs", {}))


        combined_outputs = self.outputs.definitions.copy()

        combined_outputs.update(meta.get("outputs", {}))

        # If function has a return annotation, prefer it for stdout type
        try:
            if inspect.isfunction(fn) and hasattr(fn, "__annotations__"):
                ret_ann = fn.__annotations__.get('return', None)
                if ret_ann is not None and 'stdout' not in combined_outputs:
                    combined_outputs['stdout'] = {'name': 'stdout', 'type': ret_ann}
        except Exception:
            pass


        return {"inputs": combined_inputs, "outputs": combined_outputs}


    def _schema_for_function(self, fn):

        config = self._get_combined_proc_config(fn)


        fields = []

        processed_names = set()

        sig = inspect.signature(fn)


        for name, param in sig.parameters.items():

            if name == "self":
                continue

            input_def = config["inputs"].get(name, {})

            required = param.default is inspect._empty

            if name in config["inputs"]:

                required = config["inputs"][name].get("required", required)

            type_str = (

                type_to_str(param.annotation)

                if param.annotation is not inspect._empty

                else type_to_str(input_def.get("type", Any))
            )

            entry = {"name": name, "type": type_str, "required": required}

            if input_def.get("description"):

                entry["description"] = input_def["description"]

            fields.append(entry)
            processed_names.add(name)


        for name, definition in config["inputs"].items():
            if name not in processed_names:

                entry = {
                    "name": name,

                    "type": type_to_str(definition.get("type", Any)),

                    "required": definition.get("required", False),

                }

                if definition.get("description"):

                    entry["description"] = definition["description"]

                fields.append(entry)


        output_schema = self._format_outputs_for_schema(config["outputs"])

        return {"inputs": fields, "outputs": output_schema}


    def _schema_for_class(self, clazz):
        meta = ensure_meta(clazz)

        try:
            hints = get_type_hints(clazz, include_extras=True)
        except Exception:
            hints = {}

        class_defaults = {k: v for k, v in clazz.__dict__.items() if not k.startswith("_") and not callable(v)}

        # build inputs map from annotations
        inputs_map: Dict[str, Dict[str, Any]] = {}
        for name, typ in hints.items():
            if name in ("outputs", "outputs_def"):
                continue
            required = name not in class_defaults
            inputs_map[name] = {"name": name, "type": type_to_str(typ), "required": required}

        # merge meta inputs (decorator-based), overriding when present
        for k, v in meta.get("inputs", {}).items():
            typ = v.get("type", None)
            entry_type = type_to_str(typ) if typ is not None else inputs_map.get(k, {}).get("type", "string")
            inputs_map[k] = {"name": k, "type": entry_type, "required": v.get("required", False)}
            if v.get("description"):
                inputs_map[k]["description"] = v.get("description")

        # produce ordered fields: annotated first, then extras
        input_fields: List[Dict[str, Any]] = []
        seen = set()
        for name in hints.keys():
            if name in inputs_map:
                # skip private attrs
                if name.startswith("_"):
                    continue
                input_fields.append(inputs_map[name])
                seen.add(name)
        for k, entry in inputs_map.items():
            if k not in seen and not k.startswith("_"):
                input_fields.append(entry)

        # Combina outputs de todas as fontes
        combined_outputs = self.outputs.definitions.copy()
        combined_outputs.update(meta.get("outputs", {}))

        # If class defines @MainOut or @Out methods, try to infer return types from annotations
        try:
            for name, member in inspect.getmembers(clazz, predicate=inspect.isfunction):
                # instance methods will have annotations on the function object
                if hasattr(member, '__sparkit_is_main_out__'):
                    ann = get_type_hints(member).get('return', None)
                    if ann is not None:
                        combined_outputs.setdefault('stdout', {})['type'] = ann

                    # Static analysis: if the method returns dict literals, infer fields
                    try:
                        fields = self._infer_fields_from_callable(member)
                        if fields:
                            co = combined_outputs.setdefault('stdout', {})
                            co['type'] = dict
                            co['fields'] = fields
                    except Exception:
                        pass

                if hasattr(member, '__sparkit_output_name__'):
                    out_name = getattr(member, '__sparkit_output_name__')
                    ann = get_type_hints(member).get('return', None)
                    if ann is not None:
                        combined_outputs.setdefault(out_name, {})['type'] = ann
                    # Static analysis for @Out methods: extract dict literal return shapes
                    try:
                        fields = self._infer_fields_from_callable(member)
                        if fields:
                            co = combined_outputs.setdefault(out_name, {})
                            co['type'] = dict
                            co['fields'] = fields
                    except Exception:
                        pass
        except Exception:
            # best-effort: ignore annotation parsing errors
            pass


        class_outputs_def = getattr(clazz, "outputs_def", {})

        if isinstance(class_outputs_def, dict):
            for name, definition in class_outputs_def.items():

                if name not in combined_outputs:

                    combined_outputs[name] = {

                        'name': name,

                        'type': definition.get("type", Any),

                        'description': definition.get("description"),

                    }
        elif isinstance(class_outputs_def, list):
            for o_name in class_outputs_def:

                if o_name not in combined_outputs:

                    combined_outputs[o_name] = {'name': o_name, 'type': Any}


        output_schema = self._format_outputs_for_schema(combined_outputs)

        return {"inputs": input_fields, "outputs": output_schema}

    def _run_function(self, fn):

        config = self._get_combined_proc_config(fn)

        self.outputs = OutputRegistry()

        for name, definition in config["outputs"].items():

            self.outputs.add(name, definition.get("type", Any), definition.get("description"))


        inputs_data = self._read_inputs()

        args = {}

        sig = inspect.signature(fn)

        for name, param in sig.parameters.items():

            if name == "self":
                continue
            if name in inputs_data:

                args[name] = inputs_data[name]

            elif param.default is not inspect._empty:

                args[name] = param.default

            elif config["inputs"].get(name, {}).get("required", False):

                raise ValueError(f"Missing required input: {name}")

        for k, v in config["inputs"].items():

            if k not in sig.parameters and k in inputs_data:

                args[k] = inputs_data[k]


        captured_stdout = io.StringIO()

        captured_stderr = io.StringIO()
        

        try:

            with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):

                fn(**args)

        finally:

            out_str = captured_stdout.getvalue()

            err_str = captured_stderr.getvalue()
            

            # Se o usuário não setou main_output manualmente, usamos o que foi printado

            if self.main_output is None and out_str:

                self.main_output = out_str.strip()
            

            final = {
                "stdout": self.main_output, 

                "stderr": err_str.strip() if err_str else None

            } | self.outputs.data

            print(json.dumps(final, indent=2))


    def _process_output_methods(self, instance: Any):

        main_out_found = False

        processed_outputs = set()

        for name, member in inspect.getmembers(instance, predicate=inspect.ismethod):

            if hasattr(member, '__sparkit_is_main_out__'):
                if main_out_found:

                    raise TypeError(f"Multiple @MainOut methods in {instance.__class__.__name__}.")

                main_out_found = True

                self.set_stdout(member())

            if hasattr(member, '__sparkit_output_name__'):

                output_name = getattr(member, '__sparkit_output_name__')
                if output_name in processed_outputs:

                    raise TypeError(f"Multiple @Out methods for '{output_name}'.")
                processed_outputs.add(output_name)
                if output_name not in instance.outputs.definitions:

                    raise NameError(

                        f"Output '{output_name}' was not declared. "

                        f"Add @Output('{output_name}') to the class."
                    )

                instance.outputs.set_data(output_name, member())


    def _run_class(self, clazz):

        if getattr(clazz, "__sparkit_is_node__", False) and self._procedural_registered:

            raise RuntimeError("Cannot mix OO and procedural modes.")

        self.__class__._oo_registered = True


        inputs = self._read_inputs()

        instance = clazz(**inputs)


        run_method_name = getattr(clazz, "__sparkit_meta__", {}).get('run_method')

        run_fn = None

        if run_method_name:

            run_fn = getattr(instance, run_method_name, None)

        elif hasattr(instance, "run"):

            run_fn = instance.run

        if not run_fn or not callable(run_fn):

            raise RuntimeError(f"Class '{clazz.__name__}' has no execution method.")


        captured_stdout = io.StringIO()

        captured_stderr = io.StringIO()


        try:

            with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
                run_fn()

                self._process_output_methods(instance)

        finally:

            out_str = captured_stdout.getvalue()

            err_str = captured_stderr.getvalue()


            if self.main_output is None and out_str:

                self.main_output = out_str.strip()


            final = {
                "stdout": self.main_output, 

                "stderr": err_str.strip() if err_str else None

            } | instance.outputs.data

            print(json.dumps(final, indent=2))


    def run(self, target):

        try:

            is_function = inspect.isfunction(target)


            schema = (

                self._schema_for_function(target)
                if is_function

                else self._schema_for_class(target)
            )


            if "--help" in sys.argv:

                self._print_help(schema)
                return


            if "--schema" in sys.argv:

                print(json.dumps({"schema": schema}, indent=2))
                return


            cli_inputs = self._parse_cli_args(schema)

            if not cli_inputs:

                cli_inputs = self._read_inputs()


            self._cli_override_inputs = cli_inputs

            if is_function:

                self._run_function(target)
            else:

                self._run_class(target)


        except Exception as e:

            err_info = {

                "type": e.__class__.__name__,

                "message": str(e),

                "traceback": traceback.format_exc(),

            }

            error_output = {"stdout": None, "stderr": err_info}

            print(json.dumps(error_output, indent=2))

            sys.exit(1)



# global instance

sparkit = SparkitRuntime()

set_stdout = sparkit.set_stdout