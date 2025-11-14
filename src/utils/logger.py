"""
Sistema de logging para el ETL
"""
import logging
import os
from datetime import datetime
from pathlib import Path

class ETLLogger:
    """Clase para manejar logs del ETL"""
    
    def __init__(self, name, log_dir='logs'):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Crear logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Evitar duplicados
        if not self.logger.handlers:
            # Handler para archivo
            log_file = self.log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            # Handler para consola
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Formato
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            # Agregar handlers
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def success(self, message):
        self.logger.info(f"✓ {message}")
    
    def start_process(self, process_name):
        self.logger.info(f"{'='*60}")
        self.logger.info(f"INICIANDO: {process_name}")
        self.logger.info(f"{'='*60}")
    
    def end_process(self, process_name, success=True):
        status = "COMPLETADO" if success else "FALLIDO"
        self.logger.info(f"{'='*60}")
        self.logger.info(f"{status}: {process_name}")
        self.logger.info(f"{'='*60}\n")

# Logger principal del ETL
etl_logger = ETLLogger('ETL_DW_Salud')
