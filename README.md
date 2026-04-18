# 📦 sparkit

O sparkit é uma biblioteca feita com o objetivo de ser um facilitador CLI para permitir que aplicações diversas possam interagir com código python sem precisar rodar, de fato, para saber quais as suas entradas e saídas, além de facilitar a insersão de dados e otimizar a forma como se escreve código, usando a saída padrão como forma de comunicação entre aplicativos diferentes para que outras possam capturar a saída e usar da forma como quiser.

# Intuito

Um runtime leve para execução de scripts Python com:

- 🧠 Geração automática de esquemas de entrada e saída
- 🖥️ CLI profissional (flags, help, validação)
- 🔄 Compatível com stdin/stdout (modo pipeline)
- ⚡ Execução de funções ou classes
- Uso de @Input / @Output / @Run decoradores

---

# 🚀 Instalação

```bash
git clone https://github.com/kelwp/sparkit
cd sparkit
```

Ou instale pelo pip
```bash
pip install sparkit
```

---

# ⚡ Uso rápido

## 🔹 Exemplo com classe

```python
from Sparkit import sparkit, NodeBase

class SampleMonitor(NodeBase):
    ip: str
    device_id: str | None = None

    outputs_def = {
        'meta': {'type': dict[str, str]},
        'main': {'type': dict[str, int]}
    }

    def run(self):
        self.outputs.set_data("main", {"msg": "OK"})
        sparkit.set_stdout({"teste": 1})
        self.outputs.set_data("meta", {"status": 1})

if __name__ == "__main__":
    sparkit.run(SampleMonitor)
```

---

## 🔹 Executando via CLI

```bash
python script.py --ip 192.168.0.1
```

---

## 🔹 Saída

```json
{
  "stdout": {
    "status": "ok",
    "ip": "192.168.0.1"
  },
  "stderr": null
}
```

---

# 🧠 Modos de entrada

## 1. Flags (CLI moderna)

```bash
python script.py --ip 123
```

---

## 2. JSON inline

```bash
python script.py --input '{"ip":"123"}'
```

---

## 3. Arquivo JSON

```bash
python script.py --input-file data.json
```

---

## 4. stdin (modo pipeline)

```bash
echo '{"ip":"123"}' | python script.py
```

👉 Ideal para integração com automações e orquestradores.

---

# 📖 Help automático

```bash
python script.py --help
```

Exemplo:

```
sparkit Script CLI

Inputs:
  --ip (string, required)

Other options:
  --input '<json>'
  --input-file <file>
  --schema
  --help

Outputs:
  status (string)
  ip (string)
```

---

# 🧬 Schema automático

```bash
python script.py --schema
```

Saída:

```json
{
  "schema": {
    "inputs": [{ "name": "ip", "type": "string", "required": true }],
    "outputs": [
      { "name": "status", "type": "string" },
      { "name": "ip", "type": "string" }
    ]
  }
}
```

---

# ⚙️ Tipos suportados

| Tipo    | Exemplo CLI        | Resultado   |
| ------- | ------------------ | ----------- |
| string  | `--name John`      | `"John"`    |
| number  | `--age 25`         | `25`        |
| boolean | `--active true`    | `true`      |
| array   | `--tags '[1,2,3]'` | `[1,2,3]`   |
| json    | `--data '{"a":1}'` | `{ "a":1 }` |

---

# 🧪 Execução com função

```python
from runtime import sparkitRuntime

def handler(ip: str):
    return {"status": "ok", "ip": ip}

if __name__ == "__main__":
    sparkitRuntime().run(handler)
```

---

# 🔁 Estrutura de saída

Todo script retorna:

```json
{
  "stdout": {...},
  "stderr": null
}
```

Ou em caso de erro:

```json
{
  "stdout": null,
  "stderr": {
    "type": "ValueError",
    "message": "Missing required input: ip",
    "traceback": "..."
  }
}
```

---

# 🧩 Integração com sparkit

Esse runtime foi projetado para funcionar diretamente em pipelines:

- Recebe JSON via stdin
- Retorna JSON estruturado
- Possui schema introspectivo

👉 Ideal para:

- automações
- workflows
- agentes
- execução remota

---

# 🏗️ Arquitetura

- `sparkitRuntime`
  - parsing CLI
  - leitura de inputs
  - execução
  - geração de schema
  - tratamento de erro

---

# 🔒 Validações

- Campos obrigatórios (`required`)
- Tipagem automática
- Erros estruturados
- Argumentos desconhecidos são rejeitados

---

# 💡 Filosofia

Esse SDK segue 3 princípios:

1. **Zero boilerplate**
2. **CLI primeiro**
3. **Compatível com pipelines**

---

# 🚀 Roadmap

- [ ] Autocomplete (bash/zsh)
- [ ] Logs coloridos
- [ ] Plugins
- [ ] Execução assíncrona
- [ ] Cache de execução

---

# 🤝 Contribuição

Pull requests são bem-vindos!

---

# 📄 Licença

MIT

---

## Novidades

- Corrigida a Geração de esquema para funções e classes.
- Inferência estática de campos retornados por dicionários (mostra `fields` em `stdout` e saídas customizadas quando detectado).
- Suporte total para decoradores `@Input` aplicado em classes e métodos `@Out`/`@MainOut`.
- CLI: `--schema`, `--help`, `--input`, `--input-file`.

Versão atual do pacote: `0.1.3`.
