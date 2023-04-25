import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
import pprint
import os 
from multiprocessing import Pool
import time

# # I Mise en place de requete Multi Processé : optimisation vitesse

 
# def diff(list1, list2):
#     return list(set(list1).symmetric_difference(set(list2)))

# colonnes = ["ville" ,"lien" ,"Région" ,"Département" ,"Etablissement public de coopération intercommunale (EPCI)" ,
# "Code postal (CP)" ,"Code Insee" ,"Nom des habitants" ,"Population (2019)" ,
# "Population : rang national (2019)" ,"Densité de population (2019)" ,"Taux de chômage (2019)" ,"Pavillon bleu" ,
# "Ville d'art et d'histoire" ,"Ville fleurie" ,"Ville internet" ,"Superficie (surface)" ,"Altitude min." ,
# "Altitude max." ,"Latitude" ,"Longitude"]

# if os.path.isfile('dataset\\infos.csv'): 
#     tableauInfos = pd.read_csv('dataset\\infos.csv') 
#     colonnes1 = tableauInfos['lien']
#     tableauLiens = pd.read_csv('dataset\\liensVilles.csv') 
#     colonnes2 = tableauLiens['lien']
#     listeLiens = diff(colonnes1, colonnes2) 
# else :
#     tableauInfos = DataFrame(columns=colonnes)
#     tableauInfos.to_csv('dataset\\infos.csv' , index=False)
#     tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
#     listeLiens = tableauLiens['lien']



# # print(len(listeLiens)) 

# dico = {i : "" for i in colonnes}

# # 1 Pour cette optimisation on va definir une fonction 
# def parse(lien):
#     # cpt = 0
#     dico = {i : "" for i in colonnes} # important d'initialiser le dico a chaque boucle car la par va iterer a la place de notre IF : *-*
#     req = requests.get("https://www.journaldunet.com" + lien) # deplacement
#     time.sleep(2)
#     # 2 Condition de verification du statu code de la page
#     if req.status_code == 200 :
#         with open('dataset\\infos.csv', 'a', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
#             # for lien in listeLiens : # Nous n'avon plus besoin d'iterer dans les liens car al fonction parse va s'en charger IF : *-*
#             contenu = req.content
#             soup = bs(contenu, "html.parser")

#             dico['lien'] = lien
#             dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

#             tables = soup.findAll('table', class_ = "odTable odTableAuto")

#             for i in range(len(tables)):
#                 tousLesTr = tables[i].findAll('tr') 
                
#                 for tr in tousLesTr[1:]: 
#                     cle = tr.findAll('td')[0].text
#                     valeur = tr.findAll('td')[1].text
                    
#                     if "Nom des habitants" in cle:
#                         dico["Nom des habitants"] = valeur 
#                     elif "Taux de chômage" in cle : 
#                         dico["Taux de chômage (2019)"] = valeur
#                     else : 
#                         dico[cle] = valeur
        
#             writer.writerow(dico)
#             # cpt += 1
#             # print(cpt,"/ 34 955")             
#             # print(lien)


# # 3 Rajouter notre main

# if __name__ == "__main__" : 
#     with Pool(20) as p :
#         p.map(parse,listeLiens)


# II PROPRE 

 
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

colonnes = ["ville" ,"lien" ,"Région" ,"Département" ,"Etablissement public de coopération intercommunale (EPCI)" ,
"Code postal (CP)" ,"Code Insee" ,"Nom des habitants" ,"Population (2019)" ,
"Population : rang national (2019)" ,"Densité de population (2019)" ,"Taux de chômage (2019)" ,"Pavillon bleu" ,
"Ville d'art et d'histoire" ,"Ville fleurie" ,"Ville internet" ,"Superficie (surface)" ,"Altitude min." ,
"Altitude max." ,"Latitude" ,"Longitude"]

if os.path.isfile('dataset\\infos.csv'): 
    tableauInfos = pd.read_csv('dataset\\infos.csv') # si probleme tokenizing inserer le parametre suivant : ,error_bad_lines=False
    colonnes1 = tableauInfos['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv') 
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2) 
else :
    tableauInfos = DataFrame(columns=colonnes)
    tableauInfos.to_csv('dataset\\infos.csv' , index=False)
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

# listeLiens = [lien for lien in listeLiens if lien[:11] == '/managemet'] # Gestion erreur 

dico = {i : "" for i in colonnes}

def parse(lien):
    dico = {i : "" for i in colonnes} 
    req = requests.get("https://www.journaldunet.com" + lien)
    time.sleep(2)

    if req.status_code == 200 :
        with open('dataset\\infos.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            dico['lien'] = lien
            dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            for i in range(len(tables)):
                tousLesTr = tables[i].findAll('tr') 
                
                for tr in tousLesTr[1:]: 
                    cle = tr.findAll('td')[0].text
                    valeur = tr.findAll('td')[1].text
                    
                    if "Nom des habitants" in cle:
                        dico["Nom des habitants"] = valeur 
                    elif "Taux de chômage" in cle : 
                        dico["Taux de chômage (2019)"] = valeur
                    else : 
                        dico[cle] = valeur
        
            writer.writerow(dico)
            print(lien)
            
if __name__ == "__main__" : 
    with Pool(20) as p :
        p.map(parse,listeLiens)



