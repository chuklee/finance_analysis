# Le projet d'analyse financière pour PYBD

AUTHORS : SAUNIER Vincent, NATALE Martin, CHANE-YOCK-NAM David.

## Lancer le projet :

```dockerfile
# Dans dossiers analyzer
ln -s ../../analyzer/ analyzer
make
# Dans dossiers dashboard
ln -s ../../dashboard dashboard
make

# Dans un terminal
docker-compose up db
docker-compose up analyzer dashboard
# Dossier data à  mettre dans 
docker/data
```



## Partie Analyzer :

Au tout début nous avons expérimenté de manipuler les fichiers contenants les actions.

Au fil des essais, nous avons conclu que manipuler les dataframes prenait beaucoup de temps.

La logique est la suivante : utiliser les dataframes au minimum et employer le SQL pour faire le reste des tâches.

Le procédé pour remplir la base de données est donc :

1. <u>**STOCKS TABLE(1) :**</u> Lire tous les fichiers, remplir un dataframe à partir des informations contenues dans ces fichiers. Le problème de cette approche est le fait qu'il y ait environ 50 000 fichiers par année et qu'avec une petite configuration cela était très lent. Or ce qui nous sauve à posteriori, est le fait qu'on ait trouvé comment paralléliser les traitements en utilisant la librairie intégrée à Python : multiprocess. Le traitement (ouverture, lecture, filtrage, modification,insertion en base...)  des fichiers va donc s'effectuer en parallèle en utilisant le nombre maximum de 'worker' possible sur votre machine.
   - De quoi sera composé le dataframe ?
     - Une colonne market_id (**'mid'**) qui correspond au début du nom du fichier. Ex : les actions contenues dans compB 2021-09-27 103201.317815.bz2 auront pour mid celui correspondant au numéro de marché 'compB'. 
       - <u>Remarque :</u>  Concernant les indices a priori mal rangés dans les marchés, nous avons fait des recherches et conclu que par exemple NVIDIA, malgré qu'elle soit une action du Nasdaq Cm, a bien une valeur symbolique dans la bourse Euronext https://live.euronext.com/fr/product/equities/US67066G1040-BGEM/market-information
     - Une colonne pour **'name'** (nom de l'action)
     - Une colonne company_id : **'cid'** (dont les valeurs seront toutes à 0),
     - Une colonne **'PEA'** qui dépend du nom du fichier : si il contient pea alors les valeurs de cette colonne seront 'true ' sinon 'false'
     - Une colonne last, renommée en **'Value'** 
     - Une colonne **Volume**
2. Une fois ce dataframe rempli, on l'insert dans la base de données. 
   - <u>Remarque :</u> Les colonnes du dataframe ne correspondent pas à celles de la table stocks, on modifie donc cette table <u>temporairement.</u>
3. <u>**COMPANIES TABLE :**</u> A partir des données insérées dans stocks, on créé la table companies. Pour cela on va utiliser du SQL, certaines procédures de postgresql sont bien utiles, comme par exemple `UPDATE ON CONFLICT`.
   - On regarde dans toutes les lignes de la table stocks et on récupère les symboles uniques, en utilisant un SELECT DISTINCT (SQL).
   - Arrive le cas où pour un même symbole, il y a deux ou plusieurs noms différents. On utilise alors l'utilitaire FIRST_VALUE appliqué à une liste des résultats triée par ordre décroissant pour trouver le dernier nom utilisé chronologiquement. Et on résout le conflit avec cette valeur.
   - Pour les autres colonnes, il n'y a qu'à récupérer les valeurs que le SELECT DISTINCT aura récupéré.
   - Les ISIN, SYMBOL_NF, SECTOR, Boursorama ainsi que REUTERS n'ont pas été traitées car nécessitent des appels à des API externes, ce qui pourrait poser problème de stabilité du projet.
   - Bien entendu, on met à jour toutes les informations en provenance de stocks, à savoir le PEAPME, le nom ainsi que le market_id.
   - La création de l'id se faisant automatiquement au fil des insertions grâce à la séquence, on peut passer à l'étape suivante.
4. **<u>STOCKS TABLE (2) :</u>** Une fois la table `companies` prête, il faut donc
   1. Mettre à jour les cid qui étaient premièrement insérés avec une valeur de 0
   2. Restaurer les colonnes de la table, on supprime donc pea, mid et name.
5. **<u>DAYSTOCKS TABLE :</u>** Après maintes péripéties, nous n'avons malheureusement pas réussi à implémenter le processus pour daystocks en utilisant des requêtes SQL. Les fonction `WINDOW` ainsi que `over / partition by` retournant de mauvais résultats, nous avons donc été contraints d'utiliser les dataframes. L'autre problème qui survint est que pour créer daystocks, on devait récolter les données en provenance de la table stocks en faisant une requête SQL `SELECT * FROM STOCKS`. Le problème de cette méthode est qu'il arrive des erreurs du type `Out of memory` étant donné la quantité de lignes retournées par cette méthde. Encore une fois, le multiprocessing nous sauve car permet de traiter les stocks par chunks d'une taille arbitrairement définie à 2 Million de lignes. Sur un total d'environ 150-160 Millions de lignes cela fait donc environ 80 chunks à paralléliser. Pour remplir la table daystocks, il faut construire l'agrégat de données suivant.
   - Date : pour un jour donné
   - Volume : Quel a été le pic de volume échangé ? => Max(Volume), certes il est potentiellement plus judicieux de prendre la somme totale des volumes échangés sur la journée. Mais dans le cadre du projet, les données ont été relevées de manière éparse, c'est-à-dire toutes les 15 minutes. Le choix de prendre la somme de toutes les valeurs aurait été nécessaire dans le cadre d'un relevé toutes les minutes. Ici on choisit d'approximer le cours de l'action par jour. On a donc décidé de prendre uniquement le maximum. 
   - High : Quel a été le pic de valeur ? => Max(Value). En utilisant le dashboard, on s'est apperçu à posteriori qu'il existait souvent des valeurs aberrantes qu'il fallait enlever. Par exemple un cours dont la valeur tourne aux alentours de 1000, avec 1 max à 10000000000. C'est problématique car sur le rendu visuel, cela fausse tout le graphique. On a donc du appliquer un filtre de réduction qui regarde si la valeur se trouve au dessus d'un certain seuil, et si oui ne la prend pas en compte.
   - Low : Quel a été le creux le plus bas de valeur ? => Min(Value)
   - Open : Quelle a été la première valeur ? => First_of(Value)
   - Close: Quelle a été la dernière valeur ? => Last_of(Value)

## Partie Dashboard :

Le code du dashboard a été pensé pour être clair, et il a été fait en sorte que la requête sql pour charger les données soit faite uniquement lorsque qu'on change d'entreprise dans le menu déroulant. Il a aussi été pensé pour éviter les dépendances circulaires.

Le dashboard est composé de 3 parties :
1. <u>**dataloader.py :**</u> L'engine est créé dans ce fichier pour éviter les dépendances circulaires entre le fichier bourse et celui-ci Ce fichier charge la data d'une entreprise dans un dataframe, avec les colonnes date, open, high, low, close, volume pour l'entreprise en question dans l'ordre croissant avec la date. On ajoute aussi 2 colonnes pour les bandes de bollinger. Enfin, pour le nom de l'entreprise on va l'ajouter au dataframe pour pouvoir l'afficher sur le titre du graphique avec une requête à la table comapnies. 
2. <u>**layout.py :**</u> Ce fichier contient les différents layout de chaque partie du dahsboard: 
   - D'abord le graphe en chandelier qui s'affiche pour la premiere entreprise, avec une grille et les légendes qui s'adaptent dynamiquement à l'entreprise.
   - le date-range-picker en haut a droite.
   - le menu déroulant qui permet de changer d'entreprise.
   - des boutons pour changer de type de graphique (chandelier, ligne).
   - un graphe en barre pour les volumes de l'entreprise. (c'est la feature ajoutée)
   - une table de statistiques journlières pour l'entreprise.
   La décision a été prise de faire le dashboard en mode sombre, c'est-à-dire avec un fond noir et des légendes et grilles blanches pour le design. Il y a quand même le menu déroulant et le date-range-picker qui sont en blanc. Malgré les options dash, modifier la couleur du background de ces éléments en noir n'a pas l'air de fonctionner.
3. <u>**callback.py :**</u> Ce fichier contient le code pour mettre à jour le dashboard en fonction des actions de l'utilisateur:
   - Le callback qui appelle update_graph pour mettre à jour le graphe en fonction de la date choisie par l'utilisateur sur le range-picker. On fait aussi passer en paramètre le type de graphe choisi par l'utilisateur. Attention, si on choisit une entreprise où les données sont vides, ou alors qu'on choisi une date de début supérieure a la date de fin, on affiche des composants vides. Cela se matérialise dans le code par des conditions pour vérifier si le dataframe est vide dans les callbacks/fonctions concernées.
   - On a aussi un callback pour mettre à jour l'apparence des boutons, pour que lorsqu'il soit selectionné le bouton devienne non éditable et qu'il soit grisé.
   - Il y a aussi des callbacks pour mettre à jour les volumes et la table de stat en fonction de la date.
   - Enfin, il y a un callback pour mettre à jour les données en fonction de l'entrprise choisie par l'utilisateur. On fait appel à la fonction load_data pour charger les données de l'entreprise choisie et on les stock dans un dcc.Store. Tout les autes callbacks prennent ce store en paramètre et dès qu'il change les graphes se mettent à jour.

La difficulté principale de cette partie a été de comprendre comment fonctionnait les callbacks et surtout comment organiser le code de façon à le répartir dans plusieurs fichiers. Au final le fichier bourse.py fait un appel au 3 autres fichier pour créer l'application dash. 
Ce problème d'optimisation de code est très intéressant et il est certainement possible de faire quelque chose de plus propre et plus optimisé. Nous avons quand même accordé une attention particulière à l'organisation du code.




















 





