from Sparkit import NodeBase, sparkit

class SampleMonitor(NodeBase):
    ip: str
    device_id: str

    outputs_def = {
        'meta': {'type': dict[str, str]}, 
        'main': {'type': dict[str, int]}
    }

    def run(self):
        self.outputs.set_data("main", {"msg": "OK"})
        sparkit.set_stdout({"teste": self.ip, "device_id": self.device_id})
        self.outputs.set_data("meta", {"status": 1})

if __name__ == "__main__":
    sparkit.run(SampleMonitor)