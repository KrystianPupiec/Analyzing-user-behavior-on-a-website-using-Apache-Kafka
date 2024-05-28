from kafka import KafkaConsumer, KafkaAdminClient
import matplotlib.pyplot as plt
from collections import Counter
from matplotlib.backends.backend_pdf import PdfPages
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
import time
import os
import psutil

# Konfiguracja Kafka
bootstrap_servers = ['localhost:9092']

# Funkcja do pobierania wszystkich tematów z Kafka
def get_all_topics():
    try:
        # Inicjalizacja klienta administracyjnego Kafka
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        # Pobranie listy tematów
        topics = admin_client.list_topics()
        return topics
    except Exception as e:
        print("Błąd podczas pobierania tematów Kafka:", e)
        return []

# Funkcja do odczytywania wiadomości z Kafka
def read_kafka_messages():
    all_data = {}
    try:
        # Pobranie wszystkich tematów
        topics = get_all_topics()
        if not topics:
            print("Brak dostępnych tematów.")
            return all_data
        
        # Inicjalizacja konsumenta Kafka
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', enable_auto_commit=False)
        # Subskrypcja wszystkich tematów
        consumer.subscribe(topics)
        print("Subskrybowane tematy:", topics)

        # Ustawienie czasu początkowego i timeoutu
        start_time = time.time()
        timeout = 10  # Czas w sekundach do zakończenia oczekiwania na wiadomości

        # Pętla do odczytywania wiadomości z Kafka
        while time.time() - start_time < timeout:
            # Polling wiadomości
            messages = consumer.poll(timeout_ms=1000)
            if not messages:
                print("Brak nowych wiadomości, czekam...")
                continue

            # Przetwarzanie odebranych wiadomości
            for topic_partition, records in messages.items():
                topic = topic_partition.topic
                for record in records:
                    value = record.value.decode('utf-8')
                    if topic not in all_data:
                        all_data[topic] = []
                    all_data[topic].append(value)
                    print(f'Odebrano wiadomość z tematu {topic}: {value}')
        
        # Zamknięcie konsumenta
        consumer.close()
    except Exception as e:
        print("Błąd podczas odczytywania wiadomości z Kafka:", e)
    
    return all_data

# Funkcja do agregowania danych
def aggregate_data(data):
    aggregated_data = {}
    try:
        # Agregacja danych według tematów
        for topic, values in data.items():
            counter = Counter(values)
            aggregated_data[topic] = counter
        
        print("Zagregowane dane:", aggregated_data)
    except Exception as e:
        print("Błąd podczas agregacji danych:", e)
    
    return aggregated_data

# Funkcja sprawdzająca, czy plik jest otwarty
def is_file_open(file_path):
    for proc in psutil.process_iter(['open_files']):
        try:
            if any(file.path == file_path for file in proc.info['open_files'] or []):
                return True
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return False

# Funkcja do zapisywania wykresów do pliku PDF
def save_plots_to_pdf(aggregated_data):
    pdf_filename = "raport.pdf"
    temp_plot_files = []

    # Sprawdzenie, czy plik jest otwarty
    if is_file_open(pdf_filename):
        print(f"Plik {pdf_filename} jest używany. Operacja tworzenia raportu została pominięta.")
        return
    
    try:
        # Inicjalizacja dokumentu PDF
        doc = SimpleDocTemplate(pdf_filename, pagesize=letter)
        elements = []
        
        # Rejestracja niestandardowej czcionki obsługującej polskie znaki
        pdfmetrics.registerFont(TTFont('DejaVuSans', 'DejaVuSans.ttf'))
        
        # Definiowanie stylów z niestandardową czcionką
        styles = getSampleStyleSheet()
        
        # Usunięcie istniejących stylów, jeśli są obecne
        if 'Title' in styles.byName:
            del styles.byName['Title']
        if 'Heading1' in styles.byName:
            del styles.byName['Heading1']
        if 'Heading2' in styles.byName:
            del styles.byName['Heading2']
        if 'BodyText' in styles.byName:
            del styles.byName['BodyText']
        
        # Dodanie nowych stylów
        styles.add(ParagraphStyle(name='Title', fontName='DejaVuSans', fontSize=18, leading=22, alignment=TA_CENTER))
        styles.add(ParagraphStyle(name='Heading1', fontName='DejaVuSans', fontSize=14, leading=18, alignment=TA_CENTER))
        styles.add(ParagraphStyle(name='Heading2', fontName='DejaVuSans', fontSize=12, leading=15, alignment=TA_CENTER))
        styles.add(ParagraphStyle(name='BodyText', fontName='DejaVuSans', fontSize=10, leading=12))
        
        # Dodanie tytułu do dokumentu
        title = "Raport dotyczący zainteresowania poszczególnymi usługami sieciowymi"
        header = Paragraph(title, style=styles["Title"])
        elements.append(header)
        elements.append(Spacer(1, 12))

        # Agregacja danych kliknięć według podstawowego tematu
        overall_clicks = Counter()
        for topic, counter in aggregated_data.items():
            if topic.endswith('_clicks'):
                base_topic = topic.split('_')[0]
                overall_clicks[base_topic] += sum(counter.values())

        # Tworzenie wykresu ogólnego rozkładu kliknięć
        if overall_clicks:
            labels = [f'{service} ({count})' for service, count in overall_clicks.items()]
            sizes = list(overall_clicks.values())
            plt.figure(figsize=(8, 6))
            plt.pie(sizes, labels=labels, autopct='%1.1f%%')
            plt.title('Rozkład kliknięć na poszczególne usługi')
            temp_plot_filename = 'temp_plot_overall_clicks.png'
            plt.savefig(temp_plot_filename)
            temp_plot_files.append(temp_plot_filename)
            plt.close()
            elements.append(Image(temp_plot_filename, width=400, height=300))
            elements.append(Spacer(1, 50))  # Dodajemy większy odstęp dla nowej strony
        
        # Tworzenie wykresów dla poszczególnych tematów
        topics = set(topic.split('_')[0] for topic in aggregated_data.keys())
        for _ in range(24):
            elements.append(Spacer(1, 10))
        for base_topic in topics:
            if base_topic == "home":
                continue

            # Dodanie nagłówka i odstępu
            elements.append(Paragraph(f'Zainteresowanie usługą {base_topic}', style=styles["Heading1"]))
            elements.append(Spacer(1, 12))

            for suffix in ['age', 'city', 'gender']:
                topic = f'{base_topic}_{suffix}'
                if topic in aggregated_data:
                    labels = list(aggregated_data[topic].keys())
                    counts = list(aggregated_data[topic].values())
                    
                    if not counts:  # Sprawdź, czy są jakieś dane do wizualizacji
                        continue
                    
                    # Tworzenie wykresu dla danego tematu i kategorii
                    plt.figure(figsize=(8, 6))
                    plt.pie(counts, labels=labels, autopct='%1.1f%%')
                    if suffix == 'age':
                        plt.title(f'Rozkład wieku dotyczący zainteresowania {base_topic}')
                    elif suffix == 'city':
                        plt.title(f'Rozkład miast dotyczący zainteresowania {base_topic}')
                    elif suffix == 'gender':
                        plt.title(f'Rozkład płci dotyczący zainteresowania {base_topic}')
                    temp_plot_filename = f'temp_plot_{base_topic}_{suffix}.png'
                    plt.savefig(temp_plot_filename)
                    temp_plot_files.append(temp_plot_filename)
                    plt.close()
                    
                    elements.append(Spacer(1, 12))
                    elements.append(Image(temp_plot_filename, width=400, height=300))
                    elements.append(Spacer(1, 12))

        # Zapisanie dokumentu PDF
        doc.build(elements)
        print("Raport zapisany jako", pdf_filename)
    except Exception as e:
        print("Błąd podczas zapisywania wykresów do PDF:", e)
   
    finally:
        # Usunięcie tymczasowych plików wykresów
        for temp_file in temp_plot_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

# Funkcja uruchamiająca skrypt raportujący
def run_report_script():
    while True:
        # Odczytanie danych z Kafka
        data = read_kafka_messages()
        if data:
            # Agregacja danych
            aggregated_data = aggregate_data(data)
            # Zapisanie wykresów do pliku PDF
            save_plots_to_pdf(aggregated_data)
        else:
            print("Brak danych do przetworzenia.")
        time.sleep(60)  # Czekaj 60 sekund przed ponowną próbą

# Główna funkcja uruchamiająca skrypt
if __name__ == "__main__":
    run_report_script()
