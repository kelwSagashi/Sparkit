import subprocess
import json
import os
import sys
import unittest
import tempfile

# Caminho para o Sparkit (monorepo)
# O teste está em Sparkit/Sparkit/tests/test_validation.py
# O root do Sparkit (onde fica a pasta Sparkit com o código) é .. ou ../.. dependendo da estrutura
# Estrutura: Sparkit/ (root) -> Sparkit/ (package) -> tests/
# Então PYTHONPATH deve apontar para Sparkit/ (root)
SPARKIT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

class TestSparkitInputs(unittest.TestCase):

    def run_sparkit_script(self, script_content, args=None):
        """Executa um script Sparkit em um subprocesso e retorna o JSON de saída."""
        if args is None:
            args = []
        
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode='w', encoding='utf-8') as f:
            f.write(script_content)
            script_path = f.name

        try:
            env = os.environ.copy()
            env["PYTHONPATH"] = SPARKIT_PATH
            
            process = subprocess.Popen(
                [sys.executable, script_path] + args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True
            )
            stdout, stderr = process.communicate(input="")
            
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                return {"stdout": stdout, "stderr": stderr, "exit_code": process.returncode}
        finally:
            if os.path.exists(script_path):
                os.remove(script_path)

    def test_no_inputs(self):
        """Valida que um node sem entradas roda sem erro."""
        content = """
from Sparkit import Node, Run, sparkit
@Node
class TestNode:
    @Run
    def run(self):
        print("Success")
if __name__ == "__main__":
    sparkit.run(TestNode)
"""
        result = self.run_sparkit_script(content)
        self.assertEqual(result["stdout"], "Success")
        self.assertIsNone(result["stderr"])

    def test_optional_decorated(self):
        """Valida @Input(required=False)."""
        content = """
from Sparkit import Node, Input, Run, sparkit
@Input(name="opt", required=False)
@Node
class TestNode:
    opt: str
    @Run
    def run(self):
        print(f"val:{self.opt}")
if __name__ == "__main__":
    sparkit.run(TestNode)
"""
        result = self.run_sparkit_script(content)
        self.assertEqual(result["stdout"], "val:None")
        self.assertIsNone(result["stderr"])

    def test_optional_type_hint(self):
        """Valida detecção automática de Optional[T]."""
        content = """
from Sparkit import Node, Run, sparkit
from typing import Optional
@Node
class TestNode:
    val: Optional[int]
    @Run
    def run(self):
        print(f"val:{self.val}")
if __name__ == "__main__":
    sparkit.run(TestNode)
"""
        result = self.run_sparkit_script(content)
        self.assertEqual(result["stdout"], "val:None")
        self.assertIsNone(result["stderr"])

    def test_function_optional(self):
        """Valida argumentos opcionais em funções."""
        content = """
from Sparkit import sparkit
from typing import Optional
def my_func(a: int, b: Optional[str]):
    print(f"a:{a}, b:{b}")
if __name__ == "__main__":
    import sys
    sys.argv.extend(["--a", "10"])
    sparkit.run(my_func)
"""
        result = self.run_sparkit_script(content)
        self.assertEqual(result["stdout"], "a:10, b:None")
        self.assertIsNone(result["stderr"])

    def test_required_missing_fails(self):
        """Valida que argumentos obrigatórios ainda falham se ausentes."""
        content = """
from Sparkit import sparkit
def my_func(a: int):
    print(a)
if __name__ == "__main__":
    sparkit.run(my_func)
"""
        result = self.run_sparkit_script(content)
        self.assertIsNone(result["stdout"])
        self.assertEqual(result["stderr"]["type"], "ValueError")
        self.assertIn("Missing required input: a", result["stderr"]["message"])

    def test_readme_generation(self):
        """Valida que a flag --readme gera um README.md válido."""
        content = """
from Sparkit import Node, Run, sparkit
@Node
class TestReadmeNode:
    param1: int
    @Run
    def run(self):
        pass
if __name__ == "__main__":
    sparkit.run(TestReadmeNode)
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            readme_path = os.path.join(tmpdir, "TEST_README.md")
            result = self.run_sparkit_script(content, ["--readme", "-o", readme_path])
            
            self.assertTrue(os.path.isfile(readme_path), "README file was not created")
            with open(readme_path, "r", encoding="utf-8") as f:
                readme_content = f.read()
            
            self.assertIn("# 🚀 TestReadmeNode", readme_content)
            self.assertIn("--param1", readme_content)
            self.assertIn("Como Executar", readme_content)

    def test_zip_generation(self):
        """Valida que a flag --zip gera um zip com os arquivos corretos."""
        import zipfile
        content = """
from Sparkit import Node, Run, sparkit
@Node
class TestZipNode:
    param1: int
    @Run
    def run(self):
        pass
if __name__ == "__main__":
    sparkit.run(TestZipNode)
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "test.zip")
            
            script_path = os.path.join(tmpdir, "my_script.py")
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(content)
                
            env = os.environ.copy()
            env["PYTHONPATH"] = SPARKIT_PATH
            
            process = subprocess.Popen(
                [sys.executable, script_path, "--zip", "-o", zip_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True
            )
            stdout, stderr = process.communicate()
            
            self.assertTrue(os.path.isfile(zip_path), f"Zip file was not created. Stderr: {stderr}")
            
            with zipfile.ZipFile(zip_path, "r") as zf:
                names = zf.namelist()
                
            self.assertIn("requirements.txt", names)
            self.assertIn("README.md", names)
            self.assertIn("my_script.py", names)

if __name__ == "__main__":
    unittest.main()
