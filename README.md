Le projet d'analyse financière pour PYBD

AUTHORS :

SAUNIER Vincent

NATALE Martin

CHANE YOCK NAM David

Analyzer :

Au tout début nous avons expérimenté de manipuler les fichiers contenants les actions.

Au fil des essais, nous avons conclu que manipuler les dataframes prenait beaucoup de temps.

La logique est la suivante : utiliser les dataframes au minimum et employer le SQL pour faire le reste des tâches.

Le procédé pour remplir la base de données est donc :

1. <u>**STOCKS TABLE(1) :**</u> Lire tous les fichiers, remplir un dataframe à partir des informations contenues dans ces fichiers.
   - De quoi sera composé ce dataframe ?
     - Une colonne market_id (**'mid'**) qui correspond au début du nom du fichier. Ex : les actions contenues dans compB 2021-09-27 103201.317815.bz2 auront pour mid celui correspondant au numéro de marché 'compB'. 
       - <u>Remarque :</u>  Concernant les indices a priori mal rangés dans les marchés, nous avons fait des recherches et conclu que par exemple NVIDIA, malgré qu'elle soit une action du Nasdaq Cm, a bien une valeur symbolique dans la bourse Euronext https://live.euronext.com/fr/product/equities/US67066G1040-BGEM/market-information
     - Une colonne pour **'name'** (nom de l'action)
     - Une colonne company_id : **'cid'** (dont les valeurs seront toutes à 0),
     - Une colonne **'PEA'** qui dépend du nom du fichier : si il contient pea alors les valeurs de cette colonne seront 'true ' sinon 'false'
     - Une colonne last, renommée en **'Value'** 
     - Une colonne **Volume**
2. Une fois ce dataframe rempli, on l'insert dans la base de données. 
   - <u>Remarque :</u> Les colonnes du dataframe ne correspondent pas à celles de la table stocks, on modifie donc cette table <u>temporairement.</u>
3. <u>**COMPANIES TABLE :**</u> A partir des données insérées dans stocks, on créé la table companies. Pour cela on va utiliser du SQL, certaines procédures de postgresql sont bien utiles, comme UPDATE ON CONFLICT ou encore les WINDOW function (voir liens en annexe).
   - On regarde dans toutes les lignes de la table stocks et on récupère les symboles uniques, en utilisant un SELECT DISTINCT (SQL).
   - Arrive le cas où pour un même symbole, il y a deux ou plusieurs noms différents. On utilise alors l'utilitaire FIRST_VALUE appliqué à une liste des résultats triée par ordre décroissant pour trouver le dernier nom utilisé chronologiquement. Et on résout le conflit avec cette valeur.
   - Pour les autres colonnes, il n'y a qu'à récupérer les valeurs que le SELECT DISTINCT aura récupéré.
4. **<u>STOCKS TABLE (2) :</u>** Une fois la table companies prête, il faut
   1. Mettre à jour les cid qui étaient premièrement insérés avec une valeur de 0
   2. Restaurer les colonnes de la table, on supprime donc pea, mid et name.
5. **<u>DAYSTOCKS TABLE :</u>** Après maintes péripéties, nous n'avons malheureusement pas réussi à implémenter le processus pour daystocks en SQL. Nous avons donc été contraints d'utiliser des dataframes. Or ce qui nous sauve à posteriori, est le fait qu'on ait trouvé comment paralléliser les traitements en utilisant la librairie intégrée à Python : multiprocess. Pour remplir la table daystocks, il faut construire un agrégat de données.
   - Date : pour un jour donné
   - Volume : Quel a été le pic de volume échangé ? => Max(Volume)
   - High : Quel a été le pic de valeur ? => Max(Value)
   - Low : Quel a été le creux le plus bas de valeur ? => Min(Value)
   - Open : Quelle a été la première valeur ? => First_of(Value)
   - Close: Quelle a été la dernière valeur ? => Last_of(Value)



