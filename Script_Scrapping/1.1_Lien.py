import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
import pprint

# # I Analyse balise
##_________________________________________________________________________________

# # 6 On peut maintenant initialiser notre dictionnaire:
# colonnes = ['ville','lien']
# dico = {}
# dico['ville'] = ''
# dico['lien'] = ''

# # 1 Lien 
# lien = "https://www.journaldunet.com/management/ville/index/villes?page=1"

# # 2 Requete
# req = requests.get(lien)
# contenu = req.content
# soup = bs(contenu,"html.parser")

# # 3 Nous recherchons les liens de la page, elles ont toute un <a href avec un lien dedans >
# tousLesLiens = soup.findAll('a')

# # 4 Information nb de lien catché
# print(len(tousLesLiens))

# # 5 De plus en inspectant le code et en regardant le lien assicier on peut voirqu'elles ont toute un mot de commun, ici "ville-"
# # for lien in tousLesLiens:
# #     if '/ville-' in lien['href']:
# #         print(lien.text +" : "+ lien['href'])
# #         # print(lien['href'])

# # 7 On va maintenant l'enregistrer dans notre dictionnaire on peut mettre en commentaire #5
# for lien in tousLesLiens:
#     if '/ville-' in lien['href']:
#         dico['lien'] = lien['href'][28:] # Jutilise un slice pour recuperer seulement l'extension URL interessante 
#         dico['ville'] = lien.text
#         print(dico)

# # On peut constater qu'il n'y a pas d'erreur on pass a la suite 

# #_______________________________________________________________________________#
# # II Mise en place pour remplissage csv d'un page 

# #Collone a respecter
# colonnes = ['ville','lien'] # Base de notre dataFrame

# #Creation du tableau dataframe
# tableau = DataFrame(columns=colonnes) #creation tableaux avec le dataframe order
# tableau.to_csv('dataset\\liensVilles.csv', index=False) # le convertir en csv pour pouvoir incrire dedans 


# dico = {}
# dico['ville'] = ''
# dico['lien'] = ''

# lien = "https://www.journaldunet.com/management/ville/index/villes?page=1"



# # 1 Ajout du mode ecriture sur notre fichier csv
# with open('dataset\\liensVilles.csv','a', encoding='utf-8') as csvfile:
#     writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
    
#     req = requests.get(lien)
#     contenu = req.content
    
#     soup = bs(contenu,"html.parser")
#     tousLesLiens = soup.findAll('a')
    
#     for lien in tousLesLiens:
#         if '/ville-' in lien['href']: #Technique
#             dico['lien'] = lien['href'][28:] # Jutilise un slice pour recuperer seulement l'extension URL interessante 
#             dico['ville'] = lien.text
        
#         writer.writerow(dico)        

#_______________________________________________________________________________#
# III Mise en place pour remplissage csv DES page 

colonnes = ['ville','lien'] 

tableau = DataFrame(columns=colonnes) 
tableau.to_csv('dataset\\liensVilles.csv', index=False) 

dico = {}
dico['ville'] = ''
dico['lien'] = ''

url = "https://www.journaldunet.com/management/ville/index/villes?page=" # CHangement de la variable 

with open('dataset\\liensVilles.csv','a', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
    
    # Creation de la boucle pour iterer sur les pages
    for numeroPage in range(1,701):
        
        print("Progession : ", numeroPage, "/ 701") # Affichage progression 
        
        req = requests.get(url + str(numeroPage)) # j'ajoute l'iteration pour parcourir les pages
        contenu = req.content
        
        soup = bs(contenu,"html.parser")
        tousLesLiens = soup.findAll('a')
        
        for lien in tousLesLiens:
            if '/ville-' in lien['href']: #Technique
                if not lien.text in dico['ville'] :
                    dico['lien'] = lien['href'][28:] # Jutilise un slice pour recuperer seulement l'extension URL interessante 
                    dico['ville'] = lien.text
                
                writer.writerow(dico)        





















