"""
Funções auxiliares para manipulação de arquivos SQL.
"""

from pathlib import Path
from typing import Optional


def read_sql_file(filename: str, sql_folder: Optional[str] = None) -> str:
    """
    Lê o conteúdo de um arquivo SQL.

    Args:
        filename: Nome do arquivo SQL (ex: 'bronze_td_paciente.sql')
        sql_folder: Caminho customizado para a pasta SQL.
                   Se None, usa a pasta 'sql' relativa ao projeto.

    Returns:
        Conteúdo do arquivo SQL como string.

    Raises:
        FileNotFoundError: Se o arquivo SQL não for encontrado.
    """
    if sql_folder:
        sql_path = Path(sql_folder) / filename
    else:
        project_root = Path(__file__).parent.parent
        sql_path = project_root / "sql" / filename

        if not sql_path.exists():
            project_root = Path(__file__).parent.parent.parent
            sql_path = project_root / "sql" / filename

    if not sql_path.exists():
        raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()
