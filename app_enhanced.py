import os
import logging
import re
import json
from datetime import datetime, timedelta
from functools import wraps
import hashlib
import uuid

from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import jwt
from dotenv import load_dotenv
import sqlite3

# Import our new semantic chat engine
from semantic_chat import chat_engine

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-change-this')
app.config['UPLOAD_FOLDER'] = 'uploads'

# Initialize SQLite database for user management
def init_sqlite_db():
    conn = sqlite3.connect('users.db')
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Stories table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            filename TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    # Story chunks table for semantic search
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS story_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            story_id INTEGER NOT NULL,
            chunk_text TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            embedding BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (story_id) REFERENCES stories (id)
        )
    ''')
    
    # Conversation history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conversation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            sources TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    conn.commit()
    conn.close()

init_sqlite_db()

# Initialize semantic chat engine
chat_engine.init_database()

# Helper functions
def get_db_connection():
    conn = sqlite3.connect('users.db')
    conn.row_factory = sqlite3.Row
    return conn

def generate_token(user_id):
    payload = {
        'user_id': user_id,
        'exp': datetime.utcnow() + timedelta(days=7)
    }
    return jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')

def verify_token(token):
    try:
        payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        return payload['user_id']
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            token = session.get('token')
        
        if not token:
            return jsonify({'error': 'Authentication required'}), 401
        
        user_id = verify_token(token)
        if not user_id:
            return jsonify({'error': 'Invalid or expired token'}), 401
        
        request.user_id = user_id
        return f(*args, **kwargs)
    
    return decorated_function

# Routes
@app.route('/')
def index():
    if 'token' in session:
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username'].strip()
        email = request.form['email'].strip().lower()
        password = request.form['password']
        
        # Enhanced validation
        errors = []
        
        # Username validation
        if len(username) < 3 or len(username) > 20:
            errors.append('Username must be between 3 and 20 characters')
        
        if not username.isalnum():
            errors.append('Username must contain only letters and numbers')
        
        # Email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            errors.append('Please enter a valid email address')
        
        # Password validation
        if len(password) < 6:
            errors.append('Password must be at least 6 characters')
        
        if len(errors) > 0:
            for error in errors:
                flash(error)
            return render_template('register.html')
        
        try:
            conn = get_db_connection()
            
            # Check if user exists
            existing_user = conn.execute(
                'SELECT id FROM users WHERE username = ? OR email = ?',
                (username, email)
            ).fetchone()
            
            if existing_user:
                flash('Username or email already exists')
                conn.close()
                return render_template('register.html')
            
            # Create user
            password_hash = generate_password_hash(password)
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
                (username, email, password_hash)
            )
            conn.commit()
            conn.close()
            
            logger.info(f'New user registered: {username}')
            flash('Registration successful! Please login.')
            return redirect(url_for('login'))
            
        except Exception as e:
            logger.error(f'Registration error: {str(e)}')
            flash('An error occurred during registration. Please try again.')
            return render_template('register.html')
    
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']
        
        conn = get_db_connection()
        user = conn.execute(
            'SELECT id, password_hash FROM users WHERE username = ?',
            (username,)
        ).fetchone()
        conn.close()
        
        if user and check_password_hash(user['password_hash'], password):
            token = generate_token(user['id'])
            session['token'] = token
            return redirect(url_for('dashboard'))
        
        flash('Invalid username or password')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('token', None)
    return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    if 'token' not in session:
        return redirect(url_for('login'))
    
    user_id = verify_token(session['token'])
    if not user_id:
        session.pop('token', None)
        return redirect(url_for('login'))
    
    conn = get_db_connection()
    stories = conn.execute(
        'SELECT id, title, content, created_at FROM stories WHERE user_id = ? ORDER BY created_at DESC',
        (user_id,)
    ).fetchall()
    conn.close()
    
    # Build semantic index for new stories
    chat_engine.build_semantic_index(user_id)
    
    return render_template('dashboard.html', stories=stories)

@app.route('/upload', methods=['GET', 'POST'])
def upload_story():
    if 'token' not in session:
        return redirect(url_for('login'))
    
    user_id = verify_token(session['token'])
    if not user_id:
        session.pop('token', None)
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        title = request.form['title'].strip()
        content = request.form['content'].strip()
        
        if not title or not content:
            flash('Title and content are required')
            return redirect(url_for('upload_story'))
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO stories (user_id, title, content) VALUES (?, ?, ?)',
            (user_id, title, content)
        )
        conn.commit()
        conn.close()
        
        # Rebuild semantic index after new story
        chat_engine.build_semantic_index(user_id)
        
        flash('Story uploaded successfully!')
        return redirect(url_for('dashboard'))
    
    return render_template('upload.html')

@app.route('/api/stories', methods=['GET'])
def get_stories():
    if 'token' not in session:
        return jsonify({'error': 'Authentication required'}), 401
    
    user_id = verify_token(session['token'])
    if not user_id:
        return jsonify({'error': 'Invalid token'}), 401
    
    conn = get_db_connection()
    stories = conn.execute(
        'SELECT id, title, content, created_at FROM stories WHERE user_id = ? ORDER BY created_at DESC',
        (user_id,)
    ).fetchall()
    conn.close()
    
    return jsonify([dict(story) for story in stories])

@app.route('/chat')
def chat():
    if 'token' not in session:
        return redirect(url_for('login'))
    
    user_id = verify_token(session['token'])
    if not user_id:
        session.pop('token', None)
        return redirect(url_for('login'))
    
    return render_template('chat.html')

@app.route('/delete_story/<int:story_id>', methods=['POST'])
@login_required
def delete_story(story_id):
    """Delete a story by ID"""
    conn = get_db_connection()
    
    # Verify the story belongs to the current user
    story = conn.execute(
        'SELECT id FROM stories WHERE id = ? AND user_id = ?',
        (story_id, request.user_id)
    ).fetchone()
    
    if not story:
        conn.close()
        flash('Story not found or you do not have permission to delete it.')
        return redirect(url_for('dashboard'))
    
    # Delete the story
    conn.execute('DELETE FROM stories WHERE id = ?', (story_id,))
    conn.commit()
    conn.close()
    
    # Rebuild semantic index after deletion
    chat_engine.build_semantic_index(request.user_id)
    
    flash('Story deleted successfully!')
    return redirect(url_for('dashboard'))

@app.route('/api/chat', methods=['POST'])
def chat_api():
    """Enhanced chat API with semantic search and better NLP capabilities"""
    if 'token' not in session:
        return jsonify({'error': 'Authentication required'}), 401
    
    user_id = verify_token(session['token'])
    if not user_id:
        return jsonify({'error': 'Invalid token'}), 401
    
    data = request.get_json()
    if not data or 'question' not in data:
        return jsonify({'error': 'Question is required'}), 400
    
    question = data['question'].strip()
    if not question:
        return jsonify({'error': 'Question cannot be empty'}), 400
    
    try:
        # Check if user has stories
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        story_count = cursor.execute(
            'SELECT COUNT(*) FROM stories WHERE user_id = ?',
            (user_id,)
        ).fetchone()[0]
        conn.close()
        
        if story_count == 0:
            return jsonify({
                'answer': 'You have no uploaded stories to ask questions about. Please upload some stories first.',
                'sources': [],
                'confidence': 0
            })
        
        # Build semantic index if needed
        if not chat_engine.index:
            chat_engine.build_semantic_index(user_id)
        
        # Process question using semantic search
        result = chat_engine.process_question(user_id, question)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f'Enhanced chat API error: {str(e)}')
        return jsonify({'error': 'An error occurred while processing your question'}), 500

@app.route('/api/chat/history', methods=['GET'])
@login_required
def get_chat_history():
    """Get conversation history for the current user"""
    try:
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
        history = cursor.execute('''
            SELECT question, answer, sources, created_at 
            FROM conversation_history
            WHERE user_id = ? 
            ORDER BY created_at DESC 
            LIMIT 10
        ''', (request.user_id,)).fetchall()
        
        conn.close()
        
        return jsonify([{
            'question': h[0],
            'answer': h[1],
            'sources': json.loads(h[2]) if h[2] else [],
            'timestamp': h[3]
        } for h in history])
        
    except Exception as e:
        logger.error(f'Error getting chat history: {str(e)}')
        return jsonify({'error': 'Failed to retrieve chat history'}), 500

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy', 
        'timestamp': datetime.utcnow().isoformat(),
        'semantic_engine': 'ready'
    })

if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
