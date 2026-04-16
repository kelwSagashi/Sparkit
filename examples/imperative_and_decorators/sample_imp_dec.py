# sample_proc.py
from Sparkit import sparkit, Input, Output, Run

@Input("ip", required=True, type=str)
@Input("device_id", required=False, type=str)
@Output("meta")
@Run
def main(ip, device_id=None):
    import json
    sparkit.set_stdout({"msg":"OK", "ip": ip, "id": device_id})
    sparkit.outputs.set_data("meta", {"info": 123})

if __name__ == "__main__":
    sparkit.run(main)