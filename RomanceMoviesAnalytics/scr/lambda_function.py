import json
import requests
import os
import pandas as pd
from io import StringIO
from datetime import datetime

TMDB_API_KEY = ''
CSV_PATH = r'C:\Users\maria\OneDrive\Documentos\Estagio\Sprint 5\Desafio\movies.csv'

def get_tmdb_id_from_imdb(imdb_id, headers):
    url = f"https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id"
    resposta = requests.get(url, headers=headers)
    if resposta.status_code == 200:
        data = resposta.json()
        results = data.get('movie_results', [])
        if results:
            return results[0]['id']
    return None


def buscar_dados_tmdb(imdb_id):
    base_url = "https://api.themoviedb.org/3"
    headers = {"Authorization": f"Bearer {TMDB_API_KEY}"}

    
    tmdb_id = get_tmdb_id_from_imdb(imdb_id, headers)
    if not tmdb_id:
        return {"id": imdb_id, "details": {"erro": "TMDB_ID_not_found"}, "credits": {}, "keywords": {}}

    endpoints = {
        "details": f"/movie/{tmdb_id}",
        "credits": f"/movie/{tmdb_id}/credits",
        "keywords": f"/movie/{tmdb_id}/keywords"
    }

    dados = {"id": imdb_id, "tmdb_id": tmdb_id}
    for chave, endpoint in endpoints.items():
        url = f"{base_url}{endpoint}"
        resposta = requests.get(url, headers=headers)
        dados[chave] = resposta.json() if resposta.status_code == 200 else {"erro": resposta.status_code}
    return dados


def salvar_lote_localmente(dados_lote, index):
    hoje = datetime.now()
    filename = f"romance_{index+1:03}_{hoje.year}_{hoje.month:02}_{hoje.day:02}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(dados_lote, f, indent=2, ensure_ascii=False)
    print(f"Arquivo salvo localmente: {filename}")

def main():
    try:
        df = pd.read_csv(CSV_PATH, sep='|', engine='python', on_bad_lines='skip')

        if 'genero' not in df.columns or 'id' not in df.columns:
            print("Colunas 'genero' ou 'id' não encontradas no CSV")
            return
        df_romance = df[df['genero'].str.contains('Romance', na=False, case=False)]

        if 'anoLancamento' not in df_romance.columns:
            print("Coluna 'anoLancamento' não encontrada no CSV")
            print("Colunas disponíveis:", df_romance.columns)
            return

        df_romance['anoLancamento'] = pd.to_numeric(df_romance['anoLancamento'], errors='coerce')
        df_romance = df_romance[(df_romance['anoLancamento'] >= 2000) & (df_romance['anoLancamento'] <= 2020)]

        if 'numeroVotos' not in df_romance.columns or 'notaMedia' not in df_romance.columns:
            print("Colunas 'numeroVotos' ou 'notaMedia' não encontradas no CSV")
            print("Colunas disponíveis:", df_romance.columns)
            return

        df_romance['numeroVotos'] = pd.to_numeric(df_romance['numeroVotos'], errors='coerce')
        df_romance['notaMedia'] = pd.to_numeric(df_romance['notaMedia'], errors='coerce')

        df_romance = df_romance[
            (df_romance['numeroVotos'] > 100) &
            (df_romance['notaMedia'] >= 6)
        ]

        ids = df_romance['id'].dropna().unique().tolist()
        print(f"Filmes de Romance encontrados (2000-2020): {len(ids)}")
        
        lote = []
        lote_index = 0

        for movie_id in ids:
            dados = buscar_dados_tmdb(movie_id)
            lote.append(dados)

            if len(lote) == 100:
                salvar_lote_localmente(lote, lote_index)
                print("Salvou 1 arquivo JSON para teste local. Encerrando script.")

        if lote:
            salvar_lote_localmente(lote, lote_index)
            print("Salvou arquivo (menos de 100 filmes no lote) para teste local.")

    except Exception as e:
        print(f'Erro: {str(e)}')

if __name__ == "__main__":
    main()
