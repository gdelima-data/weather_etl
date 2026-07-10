import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Garante que as variáveis de ambiente sejam carregadas
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

def get_supabase_client():
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        raise ValueError(
            "Erro: As credenciais 'SUPABASE_URL' ou 'SUPABASE_KEY' "
            "não foram encontradas nas variáveis de ambiente."
        )
    
    return create_client(supabase_url, supabase_key)