from __future__ import annotations
import io
import json
from pathlib import Path
from typing import Dict, List, Any
from docx import Document


class ContractService:
    def __init__(self, templates_root: str = "templates/contracts") -> None:
        self.templates_root = Path(templates_root)

    def list_models(self) -> List[Dict[str, Any]]:
        models = []
        for schema_path in sorted(self.templates_root.glob("*.schema.json")):
            with schema_path.open("r", encoding="utf-8") as f:
                schema = json.load(f)
            models.append(schema)
        return models

    def render_text(self, model_key: str, values: Dict[str, str]) -> str:
        template_path = self.templates_root / f"{model_key}.txt"
        template = template_path.read_text(encoding="utf-8")
        content = template
        for key, value in values.items():
            content = content.replace("{{" + key + "}}", str(value))
        return content

    def render_docx_bytes(self, model_key: str, values: Dict[str, str]) -> bytes:
        content = self.render_text(model_key, values)
        doc = Document()
        for line in content.splitlines():
            doc.add_paragraph(line)
        buffer = io.BytesIO()
        doc.save(buffer)
        return buffer.getvalue()
