from Sparkit import sparkit

def run(ip, device_id):
    sparkit.set_stdout({"msg": "OK", "ip": ip, "id": device_id})
    sparkit.outputs.set_data("meta", {"info": 123})

sparkit.outputs.add("meta")
sparkit.inputs.add('ip', required=True)
sparkit.inputs.add('device_id')

sparkit.run(run)