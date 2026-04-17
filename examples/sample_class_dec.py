from Sparkit import Node, Input, Output, Run, MainOut, Out, sparkit
import requests

@Input(name="host", required=True, type=str)
@Input(name="protocol", required=True, type=str)
@Output(name="status_code", type=int)
@Output(name="content_type", type=str)
@Node
class StatusReporter:
    host: str
    protocol: str = "https"
    
    _response: requests.Response | None = None
    _error: str | None = None

    @Run
    def fetch_status(self):
        url = f"{self.protocol}://{self.host}"
        try:
            self._response = requests.get(url, timeout=5)
            self._response.raise_for_status()
        except requests.RequestException as e:
            self._error = str(e)

    @MainOut
    def get_main_output(self) -> dict:
        if self._error:
            return {"host": self.host, "status": "error", "message": self._error}
        if self._response:
            return {"host": self.host, "status": "success", "code": self._response.status_code}
        return {"host": self.host, "status": "unknown"}

    @Out("status_code")
    def get_status_code(self):
        return self._response.status_code if self._response else -1
        
    @Out("content_type")
    def get_content_type(self):
        if self._response:
            return { "content_type": self._response.headers.get('Content-Type') }
        return { "content_type": None }


if __name__ == "__main__":
    sparkit.run(StatusReporter)