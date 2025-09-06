"""
Pipeline de embedding de documentos
Gera embeddings dos textos convertidos e armazena no Qdrant
"""

from kedro.pipeline import Node, Pipeline
from .nodes import generate_embeddings_and_upsert, generate_execution_reports


def create_pipeline(**kwargs) -> Pipeline:
    """
    Cria o pipeline de embedding com os seguintes nós:
    1. generate_embeddings_and_upsert: Gera embeddings e faz upsert no Qdrant
    2. generate_execution_reports: Gera relatórios de execução
    """
    
    # Node 1: Gera embeddings e faz upsert no Qdrant
    embedding_node = Node(
        func=generate_embeddings_and_upsert,
        inputs=["texts_partitioned", "converted_index"],  # Recebe dados do pipeline de extract
        outputs=["embedding_report_raw", "collections_used_raw"],  # Saídas intermediárias
        name="generate_embeddings_and_upsert"
    )
    
    # Node 2: Gera relatórios de execução
    reports_node = Node(
        func=generate_execution_reports,
        inputs=["embedding_report_raw", "collections_used_raw"],  # Recebe relatórios do nó anterior
        outputs=["embedding_report", "collections_used"],  # Saídas finais definidas no catalog.yml
        name="generate_execution_reports"
    )
    
    return Pipeline([embedding_node, reports_node])
