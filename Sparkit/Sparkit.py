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



# ---------- decorators ----------


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


            entry: Dict[str, Any] = {
                "name": name,

                "type": type_to_str(d.get("type", Any)),

            }


            # Descrição opcional

            if d.get("description"):

                entry["description"] = d["description"]


            # Para stdout: indica que é a saída principal

            if name == "stdout":

                entry.setdefault("description", "Main output of the node.")

                entry["nullable"] = True


            result.append(entry)

        return result


    def _get_combined_proc_config(self, fn: callable) -> dict:

        meta = getattr(fn, "__sparkit_proc_meta__", {"inputs": {}, "outputs": {}})


        combined_inputs = self.inputs.fields.copy()

        combined_inputs.update(meta.get("inputs", {}))


        combined_outputs = self.outputs.definitions.copy()

        combined_outputs.update(meta.get("outputs", {}))


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


        hints = get_type_hints(clazz, include_extras=True)

        fields = []

        class_defaults = {k: v for k, v in clazz.__dict__.items() if not k.startswith("_")}


        for name, typ in hints.items():
            if name in ("outputs", "outputs_def"):
                continue

            required = name not in class_defaults

            input_meta = {}

            if "inputs" in meta and name in meta["inputs"]:

                input_meta = meta["inputs"][name]

                required = input_meta.get("required", required)

                typ = input_meta.get("type", typ)

            entry = {"name": name, "type": type_to_str(typ), "required": required}

            if input_meta.get("description"):

                entry["description"] = input_meta["description"]

            fields.append(entry)

        if "inputs" in meta:

            for k, v in meta["inputs"].items():

                if k not in [f["name"] for f in fields]:

                    entry = {

                        "name": k,

                        "type": type_to_str(v.get("type", str)),

                        "required": v.get("required", False),

                    }

                    if v.get("description"):

                        entry["description"] = v["description"]

                    fields.append(entry)


        # Combina outputs de todas as fontes

        combined_outputs = self.outputs.definitions.copy()

        combined_outputs.update(meta.get("outputs", {}))


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

        return {"inputs": fields, "outputs": output_schema}

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