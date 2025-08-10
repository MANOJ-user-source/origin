import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
from transformers import pipeline
import sqlite3
import hashlib
import json
from typing import List, Dict, Tuple
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SemanticChatEngine:
    def __init__(self):
        """Initialize the semantic chat engine with models and database."""
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.qa_pipeline = pipeline("question-answering", 
                                  model="deepset/roberta-base-squad2")
        self.index = None
        self.story_chunks = []
        self.chunk_size = 512
        self.overlap = 50
        
    def init_database(self):
        """Initialize database tables for semantic search."""
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
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
    
    def chunk_text(self, text: str) -> List[str]:
        """Split text into overlapping chunks for better semantic search."""
        sentences = re.split(r'[.!?]+', text)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
                
            if len(current_chunk) + len(sentence) < self.chunk_size:
                current_chunk += sentence + ". "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence + ". "
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def generate_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings for given texts."""
        return self.embedding_model.encode(texts)
    
    def build_semantic_index(self, user_id: int):
        """Build FAISS index for semantic search."""
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
        # Get user's stories
        stories = cursor.execute(
            'SELECT id, title, content FROM stories WHERE user_id = ?',
            (user_id,)
        ).fetchall()
        
        if not stories:
            conn.close()
            return
        
        # Clear existing chunks for this user
        cursor.execute('''
            DELETE FROM story_chunks WHERE story_id IN (
                SELECT id FROM stories WHERE user_id = ?
            )
        ''', (user_id,))
        
        all_chunks = []
        chunk_metadata = []
        
        # Process each story
        for story in stories:
            story_id, title, content = story
            
            # Combine title and content for better context
            full_text = f"{title}\n\n{content}"
            chunks = self.chunk_text(full_text)
            
            for idx, chunk in enumerate(chunks):
                all_chunks.append(chunk)
                chunk_metadata.append({
                    'story_id': story_id,
                    'chunk_index': idx,
                    'chunk_text': chunk
                })
        
        if not all_chunks:
            conn.close()
            return
        
        # Generate embeddings
        embeddings = self.generate_embeddings(all_chunks)
        
        # Store chunks and embeddings in database
        for i, (chunk, metadata) in enumerate(zip(all_chunks, chunk_metadata)):
            embedding_blob = embeddings[i].tobytes()
            cursor.execute('''
                INSERT INTO story_chunks (story_id, chunk_text, chunk_index, embedding)
                VALUES (?, ?, ?, ?)
            ''', (metadata['story_id'], chunk, metadata['chunk_index'], embedding_blob))
        
        # Build FAISS index
        dimension = embeddings.shape[1]
        self.index = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity
        faiss.normalize_L2(embeddings)
        self.index.add(embeddings.astype('float32'))
        
        # Store chunks for retrieval
        self.story_chunks = chunk_metadata
        
        conn.commit()
        conn.close()
        
        logger.info(f"Built semantic index with {len(all_chunks)} chunks")
    
    def semantic_search(self, query: str, top_k: int = 5) -> List[Dict]:
        """Perform semantic search using embeddings."""
        if not self.index or not self.story_chunks:
            return []
        
        # Generate query embedding
        query_embedding = self.generate_embeddings([query])
        faiss.normalize_L2(query_embedding)
        
        # Search for similar chunks
        scores, indices = self.index.search(query_embedding.astype('float32'), top_k)
        
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < len(self.story_chunks):
                chunk_info = self.story_chunks[idx]
                results.append({
                    'score': float(score),
                    'chunk': chunk_info['chunk_text'],
                    'story_id': chunk_info['story_id']
                })
        
        return results
    
    def generate_answer(self, question: str, context: str) -> str:
        """Generate answer using QA model."""
        try:
            result = self.qa_pipeline(question=question, context=context)
            return result['answer']
        except Exception as e:
            logger.error(f"Error generating answer: {str(e)}")
            return "I couldn't find a specific answer to your question."
    
    def get_conversation_context(self, user_id: int, limit: int = 5) -> List[Dict]:
        """Get recent conversation history for context."""
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
        history = cursor.execute('''
            SELECT question, answer FROM conversation_history
            WHERE user_id = ? ORDER BY created_at DESC LIMIT ?
        ''', (user_id, limit)).fetchall()
        
        conn.close()
        
        return [{'question': h[0], 'answer': h[1]} for h in history]
    
    def save_conversation(self, user_id: int, question: str, answer: str, sources: List[str]):
        """Save conversation to history."""
        conn = sqlite3.connect('users.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO conversation_history (user_id, question, answer, sources)
            VALUES (?, ?, ?, ?)
        ''', (user_id, question, answer, json.dumps(sources)))
        
        conn.commit()
        conn.close()
    
    def process_question(self, user_id: int, question: str) -> Dict:
        """Process a question and return semantic answer."""
        try:
            # Build index if not exists
            if not self.index:
                self.build_semantic_index(user_id)
            
            # Perform semantic search
            search_results = self.semantic_search(question)
            
            if not search_results:
                return {
                    'answer': "I couldn't find any relevant information in your uploaded stories.",
                    'sources': []
                }
            
            # Build context from top results
            context = "\n\n".join([result['chunk'] for result in search_results[:3]])
            
            # Generate answer
            answer = self.generate_answer(question, context)
            
            # Get story titles for sources
            conn = sqlite3.connect('users.db')
            cursor = conn.cursor()
            
            story_ids = [result['story_id'] for result in search_results[:3]]
            placeholders = ','.join('?' * len(story_ids))
            stories = cursor.execute(
                f'SELECT id, title FROM stories WHERE id IN ({placeholders})',
                story_ids
            ).fetchall()
            
            conn.close()
            
            sources = [story[1] for story in stories]  # story[1] is the title column
            
            # Save conversation
            self.save_conversation(user_id, question, answer, sources)
            
            return {
                'answer': answer,
                'sources': sources,
                'confidence': max([r['score'] for r in search_results]) if search_results else 0
            }
            
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}")
            return {
                'answer': "I encountered an error processing your question. Please try again.",
                'sources': []
            }

# Global instance
chat_engine = SemanticChatEngine()
