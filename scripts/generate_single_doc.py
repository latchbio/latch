#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

try:
    import openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


class DocumentationGenerator:
    def __init__(self, model: str | None = None):
        self.model = model or "gpt-5-nano-2025-08-07"
        self.client = self._init_client()

    def _init_client(self):
        if not HAS_OPENAI:
            raise ImportError("openai package not installed. Run: pip install openai")
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        return openai.OpenAI(api_key=api_key)

    def generate_documentation(self, source_code: str, module_path: str) -> str:
        prompt = self._create_prompt(source_code, module_path)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content

    def validate_documentation(self, full_doc: str) -> str:
        prompt = f"""Review this auto-generated API documentation for consistency and markdown errors.

VALIDATION TASKS:
1. Fix broken internal references and links
2. Ensure proper markdown formatting:
   - Code blocks are properly closed with ```
   - Headers use correct # syntax
   - Inline code uses `backticks`
   - Proper line breaks between sections
3. Verify markdown table syntax if any tables exist
4. Check for unescaped special characters in markdown
5. Ensure module names match their content
6. Fix any obvious generation errors or inconsistencies
7. DO NOT add new content, only fix formatting and errors

Return the corrected markdown documentation. Preserve all content but fix formatting issues.

Documentation to review:
{full_doc}"""

        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content

    def _create_prompt(self, source_code: str, module_path: str) -> str:
        return f"""Generate valid markdown API documentation for this Python module.

STRICT RULES:
1. Document ONLY what exists in the source code
2. DO NOT invent functionality or describe behavior not in the code
3. DO NOT extrapolate about system behavior or user intentions
4. Keep descriptions factual and concise
5. Include accurate function signatures with type hints
6. Use docstrings from the code when present
7. When NO docstring exists, create brief factual description from function name, parameters, and return type
8. For examples, show only basic usage patterns evident from signatures
9. DO NOT include module path in headers - use module name only

MARKDOWN FORMATTING REQUIREMENTS:
- Convert docstrings to proper markdown paragraphs
- Wrap all code examples in ```python code blocks
- Use `backticks` for inline code (function names, parameters)
- Use proper markdown headers (###, ####) for functions/classes
- Format function signatures as ```python blocks
- Ensure proper line breaks between sections

Output structure:
- Module description from docstring (if present), or infer from imports/exports
- ### Functions section (if any functions)
- #### `function_name()` for each function
- Function description (from docstring or inferred from name/signature)
- Parameters description (from type hints and names)
- ```python code block with signature
- ### Classes section (if any classes)
- #### `ClassName` for each class
- Class description (from docstring or inferred from name/methods)
- Methods list with brief descriptions

DESCRIPTION GUIDELINES when no docstring:
- For functions: "Returns [return_type]. Takes [param_types]."
- For classes: "Class for [inferred_purpose] operations."
- Use parameter names and types to infer purpose
- Keep descriptions minimal and factual

Module: {module_path}

```python
{source_code}
```

Generate valid markdown documentation:"""


def get_user_facing_modules() -> list[str]:
    return [
        "src/latch/__init__.py",
        "src/latch/resources/tasks.py",
        "src/latch/resources/workflow.py",
        "src/latch/resources/conditional.py",
        "src/latch/resources/map_tasks.py",
        "src/latch/resources/reference_workflow.py",
        "src/latch/types/__init__.py",
        "src/latch/types/file.py",
        "src/latch/types/directory.py",
        "src/latch/types/metadata.py",
        "src/latch/types/glob.py",
        "src/latch/types/json.py",
        "src/latch/functions/messages.py",
        "src/latch/functions/operators.py",
        "src/latch/functions/secrets.py",
        "src/latch/ldata/__init__.py",
        "src/latch/registry/__init__.py",
        "src/latch/registry/project.py",
        "src/latch/account.py",
    ]


def process_module(generator: DocumentationGenerator, module_path: str) -> tuple[str, str] | None:
    path = Path(module_path)
    if not path.exists():
        print(f"  Warning: {module_path} not found")
        return None

    print(f"Processing {module_path}...")

    source_code = path.read_text()

    try:
        doc_content = generator.generate_documentation(source_code, module_path)
    except Exception as e:
        print(f"  Error generating docs: {e}")
        return None

    module_display = module_path.replace("src/latch/", "latch.").replace("/", ".").replace(".py", "")
    return module_display, doc_content


def main():
    parser = argparse.ArgumentParser(description="Generate API documentation using LLM")
    parser.add_argument("--model", help="Model to use (defaults to gpt-5-nano-2025-08-07)")
    parser.add_argument("--output-file", default="docs/api_reference.md", help="Output file path")
    args = parser.parse_args()

    if args.output_file:
        output_path = Path(args.output_file)
    else:
        output_path = Path("docs/api_reference.md")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        generator = DocumentationGenerator(model=args.model)
    except Exception as e:
        print(f"Error initializing LLM client: {e}")
        print("\nMake sure you have:")
        print("1. Installed the required package: pip install openai")
        print("2. Set the OPENAI_API_KEY environment variable")
        sys.exit(1)

    modules = get_user_facing_modules()
    print(f"Generating documentation using OpenAI/{generator.model}")
    print(f"Output file: {output_path}\n")

    doc_sections = [
        "# Latch SDK API Reference",
        "",
        "Auto-generated API documentation from source code.",
        "",
        f"*Generated using: OpenAI/{generator.model}*",
        "",
        "## Table of Contents",
        "",
    ]

    module_docs = []
    toc_entries = []

    for module_path in modules:
        result = process_module(generator, module_path)
        if result:
            module_name, doc_content = result
            module_docs.append((module_name, doc_content))
            anchor = module_name.replace(".", "").lower()
            toc_entries.append(f"- [{module_name}](#{anchor})")

    doc_sections.extend(toc_entries)
    doc_sections.append("")

    for module_name, doc_content in module_docs:
        doc_sections.extend([f"## {module_name}", "", doc_content, ""])

    full_doc = "\n".join(doc_sections)

    if not module_docs:
        print("\nValidating generated documentation...")
        try:
            full_doc = generator.validate_documentation(full_doc)
            print("Validation complete")
        except Exception as e:
            print(f"Warning: Validation failed: {e}")

    output_path.write_text(full_doc)

    print(f"\nDocumentation written to: {output_path}")
    print(f"Generated documentation for {len(module_docs)} modules")


if __name__ == "__main__":
    main()
