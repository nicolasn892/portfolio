"""
Data Ingestion Bot
------------------
Automação de processos de arquivos:
1. Detecta novos arquivos na pasta de entrada.
2. Padroniza nomes (remove espaços, adiciona timestamp).
3. Converte Excel (.xlsx) para CSV automaticamente.
4. Move para pastas organizadas por tipo.

Autor: Nicolas Martins
"""

import os
import shutil
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# --- CONFIGURAÇÃO ---
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "input_folder"
OUTPUT_DIR = BASE_DIR / "output_folder"

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [AUTOBOT] - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

class FileOrganizer:
    def __init__(self, input_path: Path, output_path: Path):
        self.input_path = input_path
        self.output_path = output_path
        self.setup_folders()

    def setup_folders(self):
        """Cria a estrutura de pastas de saída se não existir."""
        for subfolder in ["csv", "excel_archive", "json", "others"]:
            (self.output_path / subfolder).mkdir(parents=True, exist_ok=True)

    def sanitize_filename(self, filename: str) -> str:
        """
        Padroniza nomes de arquivos:
        - Minúsculas
        - Troca espaços por underline
        - Adiciona Timestamp para evitar sobrescrita
        """
        name_stem = Path(filename).stem.lower().replace(" ", "_")
        suffix = Path(filename).suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{name_stem}_{timestamp}{suffix}"

    def convert_excel_to_csv(self, file_path: Path) -> Path:
        """Converte XLSX para CSV e retorna o novo caminho."""
        try:
            df = pd.read_excel(file_path)
            new_name = file_path.stem + ".csv"
            # Salva na pasta temporária antes de mover
            temp_csv_path = file_path.parent / new_name
            df.to_csv(temp_csv_path, index=False, encoding='utf-8')
            logger.info(f"✅ Convertido com sucesso: {file_path.name} -> CSV")
            return temp_csv_path
        except Exception as e:
            logger.error(f"❌ Falha ao converter {file_path.name}: {e}")
            return None

    def process_files(self):
        """Loop principal que processa os arquivos."""
        logger.info(f"Iniciando varredura em: {self.input_path}")
        
        # Lista todos os arquivos na pasta de input
        files = [f for f in self.input_path.iterdir() if f.is_file()]

        if not files:
            logger.warning("Nenhum arquivo encontrado para processar.")
            return

        for file in files:
            try:
                # Ignora arquivos temporários ou ocultos
                if file.name.startswith("."):
                    continue

                ext = file.suffix.lower()
                clean_name = self.sanitize_filename(file.name)
                
                # Lógica de Roteamento de Arquivos
                if ext in ['.xlsx', '.xls']:
                    # 1. Converte para CSV
                    csv_file = self.convert_excel_to_csv(file)
                    if csv_file:
                        # Move o CSV novo
                        shutil.move(str(csv_file), str(self.output_path / "csv" / self.sanitize_filename(csv_file.name)))
                    
                    # 2. Move o Excel original para arquivo
                    dest_path = self.output_path / "excel_archive" / clean_name
                    shutil.move(str(file), str(dest_path))
                    logger.info(f"📦 Arquivado: {file.name} -> excel_archive")

                elif ext == '.csv':
                    dest_path = self.output_path / "csv" / clean_name
                    shutil.move(str(file), str(dest_path))
                    logger.info(f"🚚 Movido CSV: {file.name}")

                elif ext == '.json':
                    dest_path = self.output_path / "json" / clean_name
                    shutil.move(str(file), str(dest_path))
                    logger.info(f"🚚 Movido JSON: {file.name}")

                else:
                    dest_path = self.output_path / "others" / clean_name
                    shutil.move(str(file), str(dest_path))
                    logger.info(f"⚠️ Arquivo desconhecido movido: {file.name}")

            except Exception as e:
                logger.error(f"Erro crítico ao processar {file.name}: {e}")

def main():
    # Garante que as pastas existam para teste
    if not INPUT_DIR.exists():
        INPUT_DIR.mkdir()
        logger.warning(f"Pasta criada: {INPUT_DIR}. Adicione arquivos lá para testar!")
        
        # Cria um arquivo de teste automático
        df_test = pd.DataFrame({'id': [1, 2], 'valor': [100, 200]})
        df_test.to_excel(INPUT_DIR / "vendas_brutas.xlsx", index=False)
        logger.info("Arquivo de teste 'vendas_brutas.xlsx' criado automaticamente.")

    bot = FileOrganizer(INPUT_DIR, OUTPUT_DIR)
    bot.process_files()
    logger.info("--- Processo Finalizado ---")

if __name__ == "__main__":
    main()