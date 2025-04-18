import requests
import pymysql
import logging
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from pymysql import MySQLError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pdf_processor.log'),
        logging.StreamHandler()
    ]
)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация API
API_CONFIG = {
    'pdfco': {
        'base_url': 'https://api.pdf.co/v1',
        'api_key': os.getenv("PDFCO_API_KEY"),
        'endpoints': {
            'upload': '/file/upload',
            'info': '/pdf/info',
            'convert_jpg': '/pdf/convert/to/jpg',
            'optimize': '/pdf/optimize',
            'split': '/pdf/split'
        }
    },
    'ilovepdf': {
        'base_url': 'https://api.ilovepdf.com/v1',
        'api_key': os.getenv("ILOVEPDF_API_KEY"),
        'endpoints': {
            'upload': '/upload',
            'merge': '/merge',
            'pdf_to_docx': '/pdf/to/docx',
            'watermark': '/watermark',
            'compress_img': '/image/compress',
            'rotate_img': '/image/rotate',
            'convert_png': '/image/to/png'
        }
    }
}

# Конфигурация базы данных
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME"),
    'port': int(os.getenv("DB_PORT", 3306)),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

class PDFProcessor:
    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = 30
        self.max_retries = 3
        self.retry_delay = 2

    def init_db(self):
        """Инициализация структуры БД"""
        tables = {
            'processed_files': """
            CREATE TABLE IF NOT EXISTS processed_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                original_filename VARCHAR(255) NOT NULL,
                processed_at DATETIME NOT NULL,
                operations TEXT NOT NULL,
                status ENUM('processing', 'completed', 'failed') DEFAULT 'processing',
                result_path TEXT,
                error_message TEXT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            'processed_files_data': """
            CREATE TABLE IF NOT EXISTS processed_files_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_id INT NOT NULL,
                step_number INT NOT NULL,
                file_path TEXT NOT NULL,
                file_size BIGINT NOT NULL,
                file_type VARCHAR(10) NOT NULL,
                FOREIGN KEY (file_id) REFERENCES processed_files(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        }
        
        try:
            with pymysql.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    for table, sql in tables.items():
                        cursor.execute(f"SHOW TABLES LIKE '{table}'")
                        if not cursor.fetchone():
                            cursor.execute(sql)
                            logging.info(f"Создана таблица {table}")
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Ошибка инициализации БД: {str(e)}")
            return False

    def make_api_request(self, api_name, endpoint, method='POST', payload=None, files=None):
        """Универсальный метод для API запросов"""
        api_config = API_CONFIG[api_name]
        url = f"{api_config['base_url']}{api_config['endpoints'][endpoint]}"
        
        headers = {
            'x-api-key': api_config['api_key']
        } if api_name == 'pdfco' else {
            'Authorization': f"Bearer {api_config['api_key']}"
        }
        
        for attempt in range(self.max_retries):
            try:
                if method == 'POST':
                    if files:
                        response = self.session.post(url, files=files, headers=headers)
                    else:
                        response = self.session.post(url, json=payload, headers=headers)
                else:
                    response = self.session.get(url, headers=headers)
                
                response.raise_for_status()
                result = response.json()
                
                if result.get('error'):
                    raise ValueError(result.get('message', 'API returned error'))
                
                return result
                
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay)
        
        raise Exception(f"Не удалось выполнить запрос после {self.max_retries} попыток")

    def upload_file(self, file_path, api_name):
        """Загрузка файла на указанный API"""
        try:
            with open(file_path, 'rb') as f:
                result = self.make_api_request(
                    api_name=api_name,
                    endpoint='upload',
                    files={'file': (os.path.basename(file_path), f)}
                )
                return result.get('url') or result.get('file_url')
        except Exception as e:
            logging.error(f"Ошибка загрузки файла: {str(e)}")
            return None

    def get_pdf_info(self, file_url):
        """Получение информации о PDF с улучшенной проверкой страниц"""
        try:
            result = self.make_api_request(
                api_name='pdfco',
                endpoint='info',
                payload={'url': file_url}
            )
            
            pages = result.get('info', {}).get('PageCount')
            if pages < 1:
                # Альтернативный способ проверки количества страниц
                logging.warning("Основной метод не вернул количество страниц, использую альтернативный")
                try:
                    # Используем другой endpoint для проверки страниц
                    result = self.make_api_request(
                        api_name='pdfco',
                        endpoint='pdf/page/count',
                        payload={'url': file_url}
                    )
                    pages = result.get('pageCount', 0)
                except Exception as e:
                    logging.error(f"Ошибка альтернативной проверки страниц: {str(e)}")
            
            return {
                'name': result.get('name', os.path.basename(file_url)),
                'pages': pages,
                'size': result.get('size', 0)
            }
        except Exception as e:
            logging.error(f"Ошибка получения информации: {str(e)}")
            return None

    def process_pdfco_operation(self, file_url, operation):
        """Обработка файла через PDF.co API"""
        endpoints = {
            '1': 'convert_jpg',
            '2': 'optimize',
            '3': 'split'
        }

        payload={'url': file_url, 'async': False, 'inline': True}

        if operation == '3':
        # Запрашиваем диапазон страниц у пользователя
            while True:
                pages_input = input("Введите диапазон страниц для разделения (например, '1-3,5' или 'all' для всех): ").strip()
                if pages_input.lower() == 'all':
                    payload['pages'] = 'all'
                    break
                elif pages_input.replace('-','').replace(',','').isdigit():
                    payload['pages'] = pages_input
                    break
                print("Некорректный формат. Примеры: '1-3,5' или 'all'")
        try:
            result = self.make_api_request(
                api_name='pdfco',
                endpoint=endpoints[operation],
                payload=payload
            )
            return result, 'image' if operation == '1' else 'pdf'
        except Exception as e:
            logging.error(f"Ошибка обработки PDF.co: {str(e)}")
            return None, None

    def process_ilovepdf_operation(self, file_url, operation, file_type):
        """Обработка файла через ilovepdf API"""
        endpoints = {
            'pdf': {
                '1': 'merge',
                '2': 'pdf_to_docx',
                '3': 'watermark'
            },
            'image': {
                '1': 'compress_img',
                '2': 'rotate_img',
                '3': 'convert_png'
            }
        }
        
        try:
            result = self.make_api_request(
                api_name='ilovepdf',
                endpoint=endpoints[file_type][operation],
                payload={'url': file_url}
            )
            return result, 'docx' if operation == '2' and file_type == 'pdf' else file_type
        except Exception as e:
            logging.error(f"Ошибка обработки ilovepdf: {str(e)}")
            return None, None

    def download_result(self, url, save_path, api_name):
        """Скачивание результата обработки"""
        try:
            api_config = API_CONFIG[api_name]
            headers = {
                'x-api-key': api_config['api_key']
            } if api_name == 'pdfco' else {
                'Authorization': f"Bearer {api_config['api_key']}"
            }
            
            with self.session.get(url, headers=headers, stream=True) as r:
                r.raise_for_status()
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return True
        except Exception as e:
            logging.error(f"Ошибка скачивания: {str(e)}")
            return False

    def save_to_db(self, file_info, operations, result_files, error=None):
        """Сохранение результатов в БД"""
        try:
            with pymysql.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO processed_files 
                        (original_filename, processed_at, operations, status, result_path, error_message)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        file_info['filename'],
                        datetime.now(),
                        ' → '.join(operations),
                        'failed' if error else 'completed',
                        file_info['output_dir'],
                        str(error)[:500] if error else None
                    ))
                    file_id = cursor.lastrowid
                    
                    for step, files in result_files.items():
                        for file in files:
                            cursor.execute("""
                                INSERT INTO processed_files_data 
                                (file_id, step_number, file_path, file_size, file_type)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (
                                file_id,
                                step,
                                file['path'],
                                file['size'],
                                file['type']
                            ))
                conn.commit()
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения в БД: {str(e)}")
            return False

    def process_file(self, file_path):
        """Основной процесс обработки файла"""
        if not os.path.exists(file_path):
            error = f"Файл не найден: {file_path}"
            logging.error(error)
            return False, error
        
        if not file_path.lower().endswith('.pdf'):
            error = "Поддерживаются только PDF файлы"
            logging.error(error)
            return False, error
        
        # Подготовка структуры для результатов
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        output_dir = os.path.join('output', base_name)
        os.makedirs(output_dir, exist_ok=True)
        
        file_info = {
            'filename': os.path.basename(file_path),
            'output_dir': output_dir
        }
        operations = []
        result_files = {}
        
        try:
            # Шаг 1: Обработка через PDF.co
            # Загрузка файла
            file_url = self.upload_file(file_path, 'pdfco')
            if not file_url:
                raise ValueError("Не удалось загрузить файл на PDF.co")
            
            # Получение информации о PDF
            pdf_info = self.get_pdf_info(file_url) or {
                'name': os.path.basename(file_path),
                'pages': 1,
                'size': os.path.getsize(file_path)
            }
            
            # Выбор операции
            print("\nДоступные операции PDF.co:")
            print("1. Конвертировать в JPG")
            print("2. Сжать PDF")
            print("3. Разделить PDF (требуется ≥2 страниц)")
            
            while True:
                choice = input("Выберите операцию (1-3): ").strip()
                if choice in ['1', '2', '3']:
                    if choice == '3' and pdf_info['pages'] < 2:
                        print("Для разделения нужно минимум 2 страницы")
                        continue
                    break
                print("Неверный выбор!")
            
            # Выполнение операции
            result, output_type = self.process_pdfco_operation(file_url, choice)
            if not result:
                raise ValueError("Ошибка обработки файла")
            
            operations.append({
                '1': 'Конвертация в JPG',
                '2': 'Сжатие PDF',
                '3': 'Разделение PDF'
            }[choice])
            
            # Скачивание результатов
            step = 1
            urls = result.get('urls', [result.get('url')])
            
            downloaded_files = []
            for i, url in enumerate(filter(None, urls)):
                ext = '.jpg' if output_type == 'image' else '.pdf'
                save_path = os.path.join(output_dir, f"{base_name}_step{step}_{i+1}{ext}")
                
                if self.download_result(url, save_path, 'pdfco'):
                    downloaded_files.append({
                        'path': save_path,
                        'size': os.path.getsize(save_path),
                        'type': output_type
                    })
            
            if not downloaded_files:
                raise ValueError("Не удалось скачать результаты обработки")
            
            result_files[step] = downloaded_files
            current_file = downloaded_files[0]['path']
            current_type = output_type
            
            # Шаг 2: Обработка через ilovepdf
            if input("\nВыполнить обработку на ilovepdf API? (y/n): ").lower() == 'y':
                step = 2
                file_type = 'pdf' if current_type == 'pdf' else 'image'
                
                print(f"\nДоступные операции ilovepdf ({file_type}):")
                print("1. Объединить PDF" if file_type == 'pdf' else "1. Сжать изображение")
                print("2. Конвертировать в Word" if file_type == 'pdf' else "2. Повернуть изображение")
                print("3. Добавить водяной знак" if file_type == 'pdf' else "3. Конвертировать в PNG")
                
                while True:
                    choice = input("Выберите операцию (1-3): ").strip()
                    if choice in ['1', '2', '3']:
                        break
                    print("Неверный выбор!")
                
                # Загрузка файла
                file_url = self.upload_file(current_file, 'ilovepdf')
                if not file_url:
                    raise ValueError("Не удалось загрузить файл на ilovepdf")
                
                # Выполнение операции
                result, result_type = self.process_ilovepdf_operation(file_url, choice, file_type)
                if not result:
                    raise ValueError("Ошибка обработки на ilovepdf")
                
                operations.append({
                    'pdf': {
                        '1': 'Объединение PDF',
                        '2': 'Конвертация в Word',
                        '3': 'Добавление водяного знака'
                    },
                    'image': {
                        '1': 'Сжатие изображения',
                        '2': 'Поворот изображения',
                        '3': 'Конвертация в PNG'
                    }
                }[file_type][choice])
                
                # Скачивание результатов
                urls = result.get('urls', [result.get('url')])
                
                downloaded_files = []
                for i, url in enumerate(filter(None, urls)):
                    ext = {
                        'pdf': '.pdf',
                        'image': '.jpg',
                        'docx': '.docx'
                    }.get(result_type, '.bin')
                    
                    save_path = os.path.join(output_dir, f"{base_name}_step{step}_{i+1}{ext}")
                    
                    if self.download_result(url, save_path, 'ilovepdf'):
                        downloaded_files.append({
                            'path': save_path,
                            'size': os.path.getsize(save_path),
                            'type': result_type
                        })
                
                if downloaded_files:
                    result_files[step] = downloaded_files
            
            # Сохранение результатов в БД
            self.save_to_db(file_info, operations, result_files)
            return True, "Обработка завершена успешно"
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Ошибка обработки: {error_msg}")
            self.save_to_db(file_info, operations, result_files, error_msg)
            return False, error_msg

def main():
    print("\nPDF Processing Chain v3.0")
    print("="*40)
    
    processor = PDFProcessor()
    if not processor.init_db():
        print("Ошибка инициализации базы данных. Проверьте логи.")
        return
    
    input_file = input("Введите путь к PDF файлу: ").strip()
    
    start_time = time.time()
    success, message = processor.process_file(input_file)
    elapsed = time.time() - start_time
    
    if success:
        print(f"\n✅ {message}")
        print(f"Время выполнения: {elapsed:.2f} сек")
    else:
        print(f"\n❌ Ошибка: {message}")
    
    print("\nПодробности в лог-файле: pdf_processor.log")

if __name__ == "__main__":
    main()