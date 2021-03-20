# USPP - Raketa
## Instaliranje potrebnih paketa
Potrebno je imati instaliran Python 3 i pip. Svi potrebni paketi instaliraju se pokretanjem naredbe:
```
pip install -r requirements.txt
```
## Prvo pokretanje programa
Pri prvom pokretanju Jupyter bilježnice, odnosno inicijalizacijom objekta `DictionaryGraph('OPTED')` preuzima se riječnik i konstruira se graf riječnika, koji se potom sprema u SQLite bazu.
## Opis datoteka

`graph.py` - kod za rad s grafovima

`dictionary_graph.py` - kod za rad s grafovima riječnika

`OPTED.ipynb` - glavna Jupyter bilježnica

`sim_scores.ipynb,  similarity.ipynb` - Jupyter bilježnice s drugim primjerima

`docs/` - kod aplikacije za rad s grafovima, koja se može isprobati na linku https://uspp-raketa.github.io/

`requirements.txt` - lista potrebnih Python paketa