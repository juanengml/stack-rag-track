"""
Pipeline de embedding de documentos
Gera embeddings dos textos convertidos e armazena no Qdrant
"""
import logging
from typing import Dict, List, Any, Tuple
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from fastembed import TextEmbedding
import uuid
from datetime import datetime
import json

logger = logging.getLogger(__name__)


def generate_embeddings_and_upsert(
    texts_partitioned: Dict[str, str],
    converted_index: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Node 1: Lê texts_partitioned, gera embeddings e faz upsert no Qdrant
    
    Args:
        texts_partitioned: Dicionário com textos convertidos (filename -> text)
        converted_index: Índice dos arquivos convertidos
    
    Returns:
        Tupla com (embedding_report, collections_used)
    """
    logger.info(f"Iniciando geração de embeddings para {len(texts_partitioned)} documentos")
    
    # Configuração do cliente Qdrant
    qdrant_client = QdrantClient(
        host="localhost",
        port=6333
    )
    
    # Inicializar o modelo de embedding
    embedding_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
    
    # Configurações
    collection_name = "documents"
    vector_size = 384  # Tamanho do vetor para bge-small-en-v1.5
    
    # Criar coleção se não existir
    try:
        qdrant_client.get_collection(collection_name)
        logger.info(f"Coleção '{collection_name}' já existe")
    except Exception:
        logger.info(f"Criando coleção '{collection_name}'")
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE)
        )
    
    # Relatórios
    embedding_report = {
        "timestamp": datetime.now().isoformat(),
        "total_documents": len(texts_partitioned),
        "processed_documents": [],
        "failed_documents": [],
        "total_vectors_created": 0,
        "total_vectors_failed": 0
    }
    
    collections_used = {
        "timestamp": datetime.now().isoformat(),
        "collections": [collection_name],
        "collection_details": {
            collection_name: {
                "vector_size": vector_size,
                "distance_metric": "COSINE",
                "documents_added": 0,
                "vectors_added": 0
            }
        }
    }
    
    # Processar cada documento
    for filename, text_content in texts_partitioned.items():
        try:
            logger.info(f"Processando embeddings para: {filename}")
            
            # Dividir texto em chunks (se necessário)
            chunks = _split_text_into_chunks(text_content, chunk_size=1000, overlap=200)
            
            points = []
            for i, chunk in enumerate(chunks):
                # Gerar embedding para o chunk
                embedding = list(embedding_model.embed([chunk]))[0]
                
                # Criar ponto para o Qdrant
                point_id = str(uuid.uuid4())
                point = PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "filename": filename,
                        "chunk_index": i,
                        "text": chunk,
                        "total_chunks": len(chunks),
                        "source": "pdf_conversion",
                        "created_at": datetime.now().isoformat()
                    }
                )
                points.append(point)
            
            # Fazer upsert no Qdrant
            qdrant_client.upsert(
                collection_name=collection_name,
                points=points
            )
            
            # Atualizar relatórios
            embedding_report["processed_documents"].append({
                "filename": filename,
                "chunks_created": len(chunks),
                "status": "success"
            })
            embedding_report["total_vectors_created"] += len(chunks)
            
            collections_used["collection_details"][collection_name]["documents_added"] += 1
            collections_used["collection_details"][collection_name]["vectors_added"] += len(chunks)
            
            logger.info(f"Sucesso: {filename} -> {len(chunks)} chunks")
            
        except Exception as e:
            logger.error(f"Erro ao processar {filename}: {e}")
            embedding_report["failed_documents"].append({
                "filename": filename,
                "error": str(e),
                "status": "failed"
            })
            embedding_report["total_vectors_failed"] += 1
    
    logger.info(f"Embedding concluído. Sucessos: {len(embedding_report['processed_documents'])}, "
                f"Falhas: {len(embedding_report['failed_documents'])}")
    
    return embedding_report, collections_used


def _split_text_into_chunks(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Divide o texto em chunks com sobreposição
    
    Args:
        text: Texto para dividir
        chunk_size: Tamanho máximo de cada chunk
        overlap: Sobreposição entre chunks
    
    Returns:
        Lista de chunks de texto
    """
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        
        # Se não é o último chunk, tenta quebrar em uma palavra
        if end < len(text):
            # Procura por um espaço próximo ao final
            space_pos = text.rfind(' ', start, end)
            if space_pos > start:
                end = space_pos
        
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        
        # Move o início para o próximo chunk com sobreposição
        start = end - overlap if end < len(text) else end
    
    return chunks


def generate_execution_reports(
    embedding_report: Dict[str, Any],
    collections_used: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Node 2: Grava relatórios de execução
    
    Args:
        embedding_report: Relatório de processamento de embeddings
        collections_used: Relatório de coleções utilizadas
    
    Returns:
        Tupla com os relatórios formatados
    """
    logger.info("Gerando relatórios de execução")
    
    # Adicionar informações adicionais aos relatórios
    embedding_report["execution_summary"] = {
        "success_rate": (
            embedding_report["total_vectors_created"] / 
            (embedding_report["total_vectors_created"] + embedding_report["total_vectors_failed"])
            if (embedding_report["total_vectors_created"] + embedding_report["total_vectors_failed"]) > 0
            else 0
        ),
        "processing_time": datetime.now().isoformat()
    }
    
    collections_used["execution_summary"] = {
        "total_collections": len(collections_used["collections"]),
        "total_vectors_across_collections": sum(
            details["vectors_added"] 
            for details in collections_used["collection_details"].values()
        ),
        "processing_time": datetime.now().isoformat()
    }
    
    logger.info("Relatórios de execução gerados com sucesso")
    
    return embedding_report, collections_used