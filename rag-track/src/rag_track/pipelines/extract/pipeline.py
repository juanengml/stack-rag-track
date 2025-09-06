"""
Pipeline de extração de documentos PDF
Converte PDFs armazenados no S3 para arquivos de texto usando docling
"""

from kedro.pipeline import Node, Pipeline
from .nodes import list_pdfs_from_s3, convert_pdfs_to_text


def create_pipeline(**kwargs) -> Pipeline:
    """
    Cria o pipeline de extração com os seguintes nós:
    1. list_pdfs_from_s3: Lista PDFs no bucket S3
    2. convert_pdfs_to_text: Converte PDFs para texto e salva no S3
    """
    
    # Node 1: Lista PDFs no bucket S3
    list_pdfs_node = Node(
        func=list_pdfs_from_s3,
        inputs=None,  # Não tem inputs, usa configuração interna
        outputs="pdf_files_list",  # Lista de arquivos PDF encontrados
        name="list_pdfs_from_s3"
    )
    
    # Node 2: Converte PDFs para texto
    convert_pdfs_node = Node(
        func=convert_pdfs_to_text,
        inputs="pdf_files_list",  # Recebe a lista de PDFs do nó anterior
        outputs=["texts_partitioned", "converted_index"],  # Saídas definidas no catalog.yml
        name="convert_pdfs_to_text"
    )
    
    return Pipeline([list_pdfs_node, convert_pdfs_node])
