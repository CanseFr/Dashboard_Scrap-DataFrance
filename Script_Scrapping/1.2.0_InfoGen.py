import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
import pprint

# # # I Analyse balise de "UNE" page <a> info 
# ##_________________________________________________________________________________

# # 1 Ciblage du premier tableau 

# lien = "https://www.journaldunet.com/management/ville/aast/ville-64001"

# req = requests.get(lien)
# contenu = req.content
# soup = bs(contenu, "html.parser")

# tables = soup.findAll('table', class_ = "odTable odTableAuto") # *-*  Premiere recherche 

# # print(len(tables)) #4
# #--* tousLesTr = tables[0].findAll('tr')   # *-* je vais chercher des balaise dans la selection d'un balaise , "double recherche"
# # print(len(tousLesTr))

# # 3 REcuperation de tous les tableaux restant ayany les meme class_
# for i in range(len(tables)):
#     tousLesTr = tables[i].findAll('tr') # --* je remplace ma valeur zero comme on peut voir au dessus par mon iterattion 
#     for tr in tousLesTr[1:]: # je ne suohaite pas recuperer l'entete
#         # print(tr.findAll('td')[0].text) # *-* findAll sur itteration # Triple recherche 
#         # print(tr.findAll('td')[1].text) # *-* findAll sur itteration # Triple recherche 
        
#     # 2 Une fois la recherche faite on peut affecter les valeur a nos clés 
#         cle = tr.findAll('td')[0].text
#         valeur = tr.findAll('td')[1].text
            
#         print(cle,":",valeur)    


# # # II Inscription CSV 
# ##_________________________________________________________________________________

# # 2 Creation des collones: inscription de ma clé uniformisé ET lien + ville 
# colonnes = ["ville" ,"lien" ,"Région" ,"Département" ,"Etablissement public de coopération intercommunale (EPCI)" ,
# "Code postal (CP)" ,"Code Insee" ,"Nom des habitants" ,"Population (2019)" ,
# "Population : rang national (2019)" ,"Densité de population (2019)" ,"Taux de chômage (2019)" ,"Pavillon bleu" ,
# "Ville d'art et d'histoire" ,"Ville fleurie" ,"Ville internet" ,"Superficie (surface)" ,"Altitude min." ,
# "Altitude max." ,"Latitude" ,"Longitude"]

# # 3 Creation du dico et initalisation du dico 
# dico = {i : "" for i in colonnes}

# # 5 recuperation du pont , connection fichier, lecture avec panda
# tableauLiens = pd.read_csv('dataset\\liensVilles.csv')

# lien = "/management/ville/aast/ville-64001"

# req = requests.get("https://www.journaldunet.com" + lien)
# contenu = req.content
# soup = bs(contenu, "html.parser")

# # 4 Affectation du fichier pont 
# dico['lien'] = lien
# dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

# tables = soup.findAll('table', class_ = "odTable odTableAuto")

# for i in range(len(tables)):
#     tousLesTr = tables[i].findAll('tr')  
#     for tr in tousLesTr[1:]: 
#         cle = tr.findAll('td')[0].text
#         valeur = tr.findAll('td')[1].text
        
#         # 1 uniformer les clé 
#         if "Nom des habitants" in cle:
#             dico["Nom des habitants"] = valeur 
#         elif "Taux de chômage" in cle : 
#             dico["Taux de chômage (2015)"] = valeur
#         else : 
#             dico[cle] = valeur
            
# pprint(dico)    

# # III Iterrer sur tous les liens
##_________________________________________________________________________________

cpt = 0

colonnes = ["ville" ,"lien" ,"Région" ,"Département" ,"Etablissement public de coopération intercommunale (EPCI)" ,
"Code postal (CP)" ,"Code Insee" ,"Nom des habitants" ,"Population (2019)" ,
"Population : rang national (2019)" ,"Densité de population (2019)" ,"Taux de chômage (2019)" ,"Pavillon bleu" ,
"Ville d'art et d'histoire" ,"Ville fleurie" ,"Ville internet" ,"Superficie (surface)" ,"Altitude min." ,
"Altitude max." ,"Latitude" ,"Longitude"]

dico = {i : "" for i in colonnes}

# 4 Creation de infoVille.csv
tableauInfos = DataFrame(columns=colonnes)

# 5 Conversion et creration du csv  
tableauInfos.to_csv('dataset\\infos.csv' , index=False)
tableauLiens = pd.read_csv('dataset\\liensVilles.csv')

# 1 Recuperation de la liste de lien pour iteration 
listeLiens = tableauLiens['lien']

# 3 Ecriture dans le dictionnaire
with open('dataset\\infos.csv', 'a', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')

    # 2 on va iterer dessus
    for lien in listeLiens :
        req = requests.get("https://www.journaldunet.com" + lien)
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
    
        # 6 Ecriture
        writer.writerow(dico)
        cpt += 1
        print(cpt,"/ 34 955")             
        print(lien)    






