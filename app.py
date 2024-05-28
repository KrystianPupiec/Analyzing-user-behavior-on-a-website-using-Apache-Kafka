from flask import Flask, render_template, request, session, redirect, url_for, flash, jsonify
from flask_mysqldb import MySQL
import MySQLdb.cursors
import threading
import time
import schedule
import logging

app = Flask(__name__)

# Ustawienie klucza sesji
app.secret_key = 'klucz'

# Konfiguracja bazy danych MySQL
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'all_users'

# Inicjalizacja rozszerzenia MySQL
mysql = MySQL(app)

# Konfiguracja logowania dla aplikacji
#logging.basicConfig(level=logging.DEBUG)

# Definicja funkcji uruchamiającej raport
def run_report_script():
    # Import funkcji uruchamiania skryptu Raport.py
    from Raport import run_report_script
    run_report_script()

# Harmonogram do uruchamiania raportu co minutę
def job():
    run_report_script()

# Funkcja wątku do uruchamiania harmonogramu
def run_schedule():
    # Harmonogramowanie zadania co minutę
    schedule.every(1).minutes.do(job)
    while True:
        # Wykonywanie zaplanowanych zadań
        schedule.run_pending()
        # Opóźnienie, aby nie obciążać procesora
        time.sleep(1)

# Uruchamianie wątku harmonogramu
threading.Thread(target=run_schedule, daemon=True).start()

# Trasa domyślna renderująca stronę logowania lub rejestracji
@app.route('/')
def login_or_register():
    return render_template('login_or_register.html')

# Trasa wyświetlająca stronę główną po zalogowaniu
@app.route('/index')
def index():
    # Sprawdzenie, czy użytkownik jest zalogowany
    if 'loggedin' not in session:
        return redirect(url_for('login_or_register'))
    
    # Pobranie danych użytkownika z sesji
    user_age = session.get('age')
    user_city = session.get('city')
    user_gender = session.get('gender')
    
    # Renderowanie strony głównej z danymi użytkownika
    return render_template('index.html', user_age=user_age, user_city=user_city, user_gender=user_gender)

# Trasa obsługująca logowanie użytkownika
@app.route('/login', methods=['POST'])
def login():
    if request.method == 'POST' and 'login_email' in request.form and 'login_password' in request.form:
        # Pobranie danych logowania z formularza
        email = request.form['login_email']
        password = request.form['login_password']
        
        # Sprawdzenie danych logowania w bazie danych
        cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('SELECT email, age, city, gender FROM users WHERE email = %s AND password = %s', (email, password,))
        user = cursor.fetchone()
        
        if user:
            # Zapisanie danych użytkownika w sesji
            session['loggedin'] = True
            session['email'] = user['email']
            session['age'] = user['age']
            session['city'] = user['city']
            session['gender'] = user['gender']
            return redirect(url_for('index'))
        else:
            # Komunikat o błędnych danych logowania
            flash('Błędne dane logowania', 'error')
            return redirect(url_for('login_or_register'))
    else:
        # Komunikat o błędzie logowania
        flash('Błąd logowania. Proszę spróbować ponownie.', 'error')
        return redirect(url_for('login_or_register'))

# Trasa wylogowująca użytkownika
@app.route('/logout')
def logout():
    # Usunięcie danych użytkownika z sesji
    session.pop('loggedin', None)
    session.pop('email', None)
    session.pop('age', None)
    session.pop('city', None)
    session.pop('gender', None)
    return redirect(url_for('login_or_register'))

# Trasa obsługująca rejestrację użytkownika
@app.route('/register', methods=['POST'])
def register():
    # Pobranie danych rejestracyjnych z formularza
    name = request.form['reg_name']
    email = request.form['email']
    age = request.form['age']
    city = request.form['city']
    password = request.form['password']

    # Dodanie nowego użytkownika do bazy danych
    cur = mysql.connection.cursor()
    cur.execute("INSERT INTO users (name, email, age, city, password) VALUES (%s, %s, %s, %s, %s)", (name, email, age, city, password))
    mysql.connection.commit()
    cur.close()

    # Komunikat o pomyślnej rejestracji
    flash('Rejestracja zakończona pomyślnie. Możesz się teraz zalogować.', 'success')
    return redirect(url_for('login_or_register'))

# Uruchomienie aplikacji Flask
if __name__ == '__main__':
    app.run(debug=True)
