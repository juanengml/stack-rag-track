"""
Pipeline de extração de documentos PDF
Converte PDFs armazenados no S3 para arquivos de texto usando docling
"""
import logging
from typing import Dict, List, Any
import boto3
from botocore.exceptions import ClientError
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
import tempfile
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def list_pdfs_from_s3(bucket_name: str = "meu-bucket", prefix: str = "raw/pdfs/") -> List[str]:
    """
    Node 1: Lista todos os arquivos PDF no bucket S3 especificado
    
    Args:
        bucket_name: Nome do bucket S3
        prefix: Prefixo do caminho no bucket (ex: "raw/pdfs/")
    
    Returns:
        Lista de chaves (caminhos) dos arquivos PDF encontrados
    """
    logger.info(f"Listando PDFs no bucket {bucket_name} com prefixo {prefix}")
    
    try:
        # Configuração do cliente S3 para MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://192.168.0.48:9300',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        
        # Lista objetos no bucket
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        pdf_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.lower().endswith('.pdf'):
                    pdf_files.append(key)
                    logger.info(f"PDF encontrado: {key}")
        
        logger.info(f"Total de PDFs encontrados: {len(pdf_files)}")
        return pdf_files
        
    except ClientError as e:
        logger.error(f"Erro ao acessar o bucket S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao listar PDFs: {e}")
        raise


def convert_pdfs_to_text(pdf_files: List[str], bucket_name: str = "meu-bucket") -> Dict[str, Any]:
    """
    Node 2: Converte PDFs para arquivos de texto usando docling
    
    Args:
        pdf_files: Lista de chaves dos arquivos PDF no S3
        bucket_name: Nome do bucket S3
    
    Returns:
        Dicionário com:
        - texts_partitioned: Dados dos textos convertidos
        - converted_index: Índice dos arquivos convertidos
    """
    logger.info(f"Iniciando conversão de {len(pdf_files)} PDFs")
    
    # Configuração do cliente S3
    s3_client = boto3.client(
        's3',
        endpoint_url='http://192.168.0.48:9300',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    
    # Inicializar o conversor docling
    converter = DocumentConverter()
    
    texts_data = {}
    converted_index = {
        "converted_files": [],
        "failed_files": [],
        "total_processed": 0,
        "total_successful": 0,
        "total_failed": 0
    }
    
    for pdf_key in pdf_files:
        try:
            logger.info(f"Processando: {pdf_key}")
            
            # Baixar PDF do S3 para arquivo temporário
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
                s3_client.download_file(bucket_name, pdf_key, temp_pdf.name)
                
                # Converter PDF para texto usando docling
                result = converter.convert(temp_pdf.name)
                
                # Extrair texto do resultado
                text_content = result.document.export_to_markdown()
                
                # Gerar nome do arquivo de saída
                pdf_filename = Path(pdf_key).stem
                output_key = f"intermedio/txt/{pdf_filename}.txt"
                
                # Salvar texto no S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=output_key,
                    Body=text_content.encode('utf-8'),
                    ContentType='text/plain'
                )
                
                # Armazenar dados para o dataset particionado
                texts_data[pdf_filename] = text_content
                
                # Atualizar índice
                converted_index["converted_files"].append({
                    "original_pdf": pdf_key,
                    "output_txt": output_key,
                    "filename": pdf_filename,
                    "status": "success"
                })
                converted_index["total_successful"] += 1
                
                logger.info(f"Convertido com sucesso: {pdf_key} -> {output_key}")
                
            # Limpar arquivo temporário
            os.unlink(temp_pdf.name)
            
        except Exception as e:
            logger.error(f"Erro ao converter {pdf_key}: {e}")
            converted_index["failed_files"].append({
                "original_pdf": pdf_key,
                "error": str(e),
                "status": "failed"
            })
            converted_index["total_failed"] += 1
        
        converted_index["total_processed"] += 1
    
    logger.info(f"Conversão concluída. Sucessos: {converted_index['total_successful']}, "
                f"Falhas: {converted_index['total_failed']}")
    
    return {
        "texts_partitioned": texts_data,
        "converted_index": converted_index
    }