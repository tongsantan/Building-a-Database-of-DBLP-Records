# SQL script to load datasets into mySQL database
# NOTE
# stands for comments
-- stands for codes in hold, undashed when needed

SET GLOBAL max_allowed_packet=134217728;
SET GLOBAL innodb_buffer_pool_size=402653184;

-- DROP database project_db_raw_load;
CREATE database project_db_raw_load;

USE project_db_raw_load;
-- DROP TABLE RawData;
CREATE TABLE RawData (PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));

#Key this in mysql Command Line Client 
-- show global variables like 'local_infile';
-- set global local_infile=true;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned1.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned2.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned3.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned4.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned5.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned6.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned7.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned8.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned9.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned10.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned11.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned12.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/df_merged_cleaned13.csv' INTO TABLE RawData
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

-- select COUNT(*) from RawData;

# Data Transformation Pipeline (Publication Table - Part I)
-- DROP TABLE tempPublication;
CREATE TABLE tempPublication (PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));

# Insert desired raw data into temporary table tempPublication
INSERT INTO tempPublication (select * from (select * from RawData
WHERE (PubType LIKE 'article%' OR PubType LIKE 'book%' OR PubType LIKE 'incollection%' OR PubType LIKE 'inproceedings%' OR PubType LIKE 'proceedings%')
AND (PubKey LIKE 'conf%' OR PubKey LIKE 'books%' OR PubKey LIKE 'journals%')) subset
WHERE Field in ('year', 'title'));
-- select * from tempPublication;

-- DROP TABLE tempPublication2;
CREATE TABLE tempPublication2 (PubKey varchar (255) NOT NULL, PubType varchar (255), Title varchar (500), Year varchar (255));

# Insert pivoted fields into temPublication2 table
# Counter added to track multiple records of the same PubKey and ensure they are still intact after pivoting

INSERT INTO tempPublication2 (SELECT PubKey, PubType,
MAX(case when Field = 'title' then Entry else NULL end) Title,
MAX(case when Field = 'year' then Entry else NULL end) Year
FROM (SELECT ROW_NUMBER() OVER(PARTITION BY PubKey, Field ORDER BY PubKey) AS Counter, PubKey, PubType, Field, Entry FROM tempPublication
GROUP BY PubKey, PubType, Field, Entry) a
GROUP BY PubKey, PubType, Counter);

-- select * from tempPublication2;

DROP TABLE tempPublication;

# Data Transformation Pipeline (tempPubLink Table)
-- DROP TABLE tempPubLink;
CREATE TABLE tempPubLink (PubKey varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));

# Insert into tempPubLink table after subsetting of desired data without author field
INSERT INTO tempPubLink (select * from (select * from RawData
WHERE (PubType LIKE 'article%' OR PubType LIKE 'book%' OR PubType LIKE 'incollection%' OR PubType LIKE 'inproceedings%' OR PubType LIKE 'proceedings%')
AND (PubKey LIKE 'conf%' OR PubKey LIKE 'books%' OR PubKey LIKE 'journals%')) subset
WHERE Field <> 'author');
-- select COUNT(*) from tempPubLink;

# Clone another table of temPubLink and Drop unnecessary columns except PubKey
-- DROP TABLE tempPubLink2;
CREATE TABLE tempPubLink2 AS SELECT * FROM tempPubLink;
ALTER TABLE tempPubLink2 DROP COLUMN Field, DROP COLUMN Entry, DROP COLUMN PubType;
-- select * from tempPubLink2;

# Create an impt tempPubLink_copy Table that consists only unique PubID and PubKey after dropping duplicated PubKey.
-- DROP TABLE tempPubLink_copy;
CREATE TABLE tempPubLink_copy (PubID INT NOT NULL AUTO_INCREMENT, PubKey varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (PubID, PubKey));
INSERT INTO tempPubLink_copy
(SELECT ROW_NUMBER() OVER(ORDER BY PubKey), PubKey FROM tempPubLink2
GROUP BY PubKey);
-- select * from tempPubLink_copy;

# Data Transformation Pipeline (Publication Table - Part 2)
-- DROP TABLE Publication;
CREATE TABLE Publication (PubID INT NOT NULL, PubKey varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, Title varchar (500), Year varchar (255), PRIMARY KEY(PubID, PubKey));

# Insert desired pivoted fields into Publication table with PubID and PubKey
INSERT INTO Publication (SELECT PubID, TPL.PubKey, Title, Year
from tempPubLink_copy TPL INNER JOIN tempPublication2 TP ON TPL.PubKey = TP.PubKey);

## Add UNIQUE constraints to PubKey
ALTER TABLE Publication ADD UNIQUE (PubKey);

# Sanity Checks (Publication - No Records with multiple data of same PubID observed)
-- SELECT DISTINCT PubID FROM Publication GROUP BY PubID HAVING count(Title) > 1 LIMIT 4; # No multiple different Title records
-- SELECT DISTINCT PubID FROM Publication GROUP BY PubID HAVING count(Year) > 1 LIMIT 4; # No multiple different Year records

-- SELECT COUNT(DISTINCT PubKey) FROM Publication;
-- SELECT COUNT(DISTINCT PubID) FROM Publication;

# Create an impt PubLink Table that holds all records with their PubIDs.
-- DROP TABLE PubLink;
CREATE TABLE PubLink (PubID INT NOT NULL, PubKey varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));
INSERT INTO PubLink 
(SELECT PubID, TPLC.PubKey, Field, Entry, PubType FROM tempPubLink_copy TPLC LEFT JOIN tempPubLink TPL ON TPLC.PubKey = TPL.PubKey);

-- select COUNT(DISTINCT PubKey) FROM PubLink;

# Data Transformation Pipeline (Book Table)
-- DROP TABLE tempBook;
CREATE TABLE tempBook (PubID INT NOT NULL, PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));
INSERT INTO tempBook (SELECT * FROM PubLink
WHERE PubType LIKE 'book%' AND Field in ('title', 'publisher', 'isbn'));
-- select COUNT(DISTINCT PubID) from tempBook;

# Drop unnecessary columns except PubID, Field and Entry
ALTER TABLE tempBook DROP COLUMN PubKey, DROP COLUMN PubType;

-- DROP TABLE Book;
CREATE TABLE Book (PubID INT NOT NULL, Title varchar (500), ISBN varchar (255), Publisher varchar (255));

# Sanity Checks (tempBook - Records with multiple different ISBNs of same PubID observed)
-- SELECT DISTINCT PubID FROM tempBook WHERE Field LIKE 'isbn' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # Have multiple different ISBN records
-- SELECT DISTINCT PubID FROM tempBook WHERE Field LIKE 'publisher' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different Publisher records
-- SELECT * FROM tempBook WHERE PubID in ('116', '129', '135', '136');

# Insert desired pivoted fields into an impt Book table with PubID Referencing Publication Table
# Counter added to track multiple records of the same PubID and ensure they are still intact after pivoting

INSERT INTO Book (SELECT PubID,   
MAX(case when Field = 'title' then Entry else NULL end) Title,
MAX(case when Field = 'isbn' then Entry else NULL end) ISBN,
MAX(case when Field = 'publisher' then Entry else NULL end) Publisher
FROM (SELECT ROW_NUMBER() OVER(PARTITION BY PubID, Field ORDER BY PubID) AS Counter, PubID, Field, Entry FROM tempBook
GROUP BY PubID, Field, Entry) a
GROUP BY PubID, Counter);

# Drop Title column and add Foreign Key constraints
ALTER TABLE Book DROP COLUMN Title, ADD FOREIGN KEY (PubID) REFERENCES Publication(PubID);
-- select * from Book;

# Data Transformation Pipeline (Incollection Table)
-- DROP TABLE tempIncollection;
CREATE TABLE tempIncollection (PubID INT NOT NULL, PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));
INSERT INTO tempIncollection (SELECT * FROM PubLink
WHERE PubType LIKE 'incollection%' AND Field in ('title', 'booktitle', 'crossref'));
-- select COUNT(DISTINCT PubID) from tempIncollection;

# Drop unnecessary columns except PubID, Field and Entry
ALTER TABLE tempIncollection DROP COLUMN PubType, DROP COLUMN PubKey;

-- DROP TABLE Incollection;
CREATE TABLE Incollection (PubID INT NOT NULL, Title varchar (500), BookTitle varchar (255), Crossref varchar (255));

# Sanity Checks (tempIncollection - No Records with multiple data of same PubID observed)
-- SELECT DISTINCT PubID FROM tempIncollection WHERE Field LIKE 'booktitle' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different BookTitle records
-- SELECT DISTINCT PubID FROM tempIncollection WHERE Field LIKE 'crossref' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different Crossref records

# Insert desired pivoted fields into an impt Incollection table with PubID Referencing Publication Table

INSERT INTO Incollection (SELECT PubID,  
MAX(case when Field = 'title' then Entry else NULL end) Title,
MAX(case when Field = 'booktitle' then Entry else NULL end) BookTitle,
MAX(case when Field = 'crossref' then Entry else NULL end) Crossref
FROM (SELECT ROW_NUMBER() OVER(PARTITION BY PubID, Field ORDER BY PubID) AS Counter, PubID, Field, Entry FROM tempIncollection
GROUP BY PubID, Field, Entry) a
GROUP BY PubID, Counter);

# Drop Title column and add Foreign Key constraints
ALTER TABLE Incollection DROP COLUMN Title, ADD FOREIGN KEY (PubID) REFERENCES Publication(PubID);
## made crossref case-sensitive because crossref of incollection references pubkey of book
ALTER TABLE Incollection
MODIFY COLUMN Crossref varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

## Add Foreign Key constraints for Crossref to PubKey
ALTER TABLE Incollection ADD FOREIGN KEY (Crossref) REFERENCES Publication(PubKey);

-- SELECT * from Incollection;

# Data Transformation Pipeline (Inproceedings Table)
-- DROP TABLE tempInproceedings;
CREATE TABLE tempInproceedings (PubID INT NOT NULL, PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));
INSERT INTO tempInproceedings (SELECT * FROM PubLink
WHERE PubType LIKE 'inproceedings%' AND Field in ('title', 'booktitle', 'crossref'));
-- select COUNT(DISTINCT PubID) from tempInproceedings;

# Drop unnecessary columns except PubID, Field and Entry
ALTER TABLE tempInproceedings DROP COLUMN PubType, DROP COLUMN PubKey;

-- DROP TABLE Inproceedings;
CREATE TABLE Inproceedings (PubID INT NOT NULL, Title varchar (500), BookTitle varchar (255), Crossref varchar (255));

# Sanity Checks (tempInproceedings - Records with multiple different Crossrefs of same PubID observed)
-- SELECT DISTINCT PubID FROM tempInproceedings WHERE Field LIKE 'booktitle' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different Booktitle records
-- SELECT DISTINCT PubID FROM tempInproceedings WHERE Field LIKE 'crossref' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # Have multiple different Crossref records
-- SELECT * FROM tempInproceedings WHERE PubID in ('168652');

# Insert desired pivoted fields into an impt Inproceedings table with PubID Referencing Publication Table
# Counter added to track multiple records of the same PubID and ensure they are still intact after pivoting

INSERT INTO Inproceedings (SELECT  PubID,
MAX(case when Field = 'title' then Entry else NULL end) Title,
MAX(case when Field = 'booktitle' then Entry else NULL end) BookTitle,
MAX(case when Field = 'crossref' then Entry else NULL end) Crossref
FROM (SELECT ROW_NUMBER() OVER(PARTITION BY PubID, Field ORDER BY PubID) AS Counter, PubID, Field, Entry FROM tempInproceedings
GROUP BY PubID, Field, Entry) a
GROUP BY PubID, Counter);

# Drop Title column and add Foreign Key constraints
ALTER TABLE Inproceedings DROP COLUMN Title, ADD FOREIGN KEY (PubID) REFERENCES Publication(PubID);
## made crossref case-sensitive because crossref of inproceedings references pubkey of proceedings 
ALTER TABLE Inproceedings
MODIFY COLUMN Crossref varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

## Add Foreign Key constraints for Crossref to PubKey
ALTER TABLE Inproceedings ADD FOREIGN KEY (Crossref) REFERENCES Publication(PubKey);

-- SELECT * FROM Inproceedings;

# Data Transformation Pipeline (Proceedings Table)
-- DROP TABLE tempProceedings;
CREATE TABLE tempProceedings (PubID INT NOT NULL, PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));
INSERT INTO tempProceedings (SELECT * FROM PubLink
WHERE PubType LIKE 'proceedings%' AND Field in ('title', 'booktitle', 'series'));
-- select COUNT(DISTINCT PubID) from tempProceedings;

# Drop unnecessary columns except PubID, Field and Entry
ALTER TABLE tempProceedings DROP COLUMN PubType, DROP COLUMN PubKey;

-- DROP TABLE Proceedings;
CREATE TABLE Proceedings (PubID INT NOT NULL, Title varchar (500), BookTitle varchar (500), Series varchar (500));

# Sanity Checks (tempProceedings - Records with multiple different series of same PubID observed)
-- SELECT DISTINCT PubID FROM tempProceedings WHERE Field LIKE 'booktitle' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different Booktitle records
-- SELECT DISTINCT PubID FROM tempProceedings WHERE Field LIKE 'series' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # Have multiple different Series records
-- SELECT * FROM tempProceedings WHERE PubID in ( '1073518');

# Insert desired pivoted fields into an impt Proceedings table with PubID Referencing Publication Table
# Counter added to track multiple records of the same PubID and ensure they are still intact after pivoting

INSERT INTO Proceedings (SELECT PubID,  
MAX(case when Field = 'title' then Entry else NULL end) Title,
MAX(case when Field = 'booktitle' then Entry else NULL end) BookTitle,
MAX(case when Field = 'series' then Entry else NULL end) Series
FROM (SELECT ROW_NUMBER() OVER(PARTITION BY PubID, Field ORDER BY PubID) AS Counter, PubID, Field, Entry FROM tempProceedings
GROUP BY PubID, Field, Entry) a
GROUP BY PubID, Counter);

# Drop Title column and add Foreign Key constraints
ALTER TABLE Proceedings DROP COLUMN Title, ADD FOREIGN KEY (PubID) REFERENCES Publication(PubID);
-- SELECT * from Proceedings;

# Data Transformation Pipeline (Article Table)
-- DROP TABLE tempArticle;
CREATE TABLE tempArticle (PubID INT NOT NULL, PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));
INSERT INTO tempArticle (SELECT * FROM PubLink
WHERE PubType LIKE 'article%' AND Field in ('title', 'journal', 'volume', 'number', 'pages'));
-- select COUNT(DISTINCT PubID) from tempArticle;

# Drop unnecessary columns except PubID, Field and Entry
ALTER TABLE tempArticle DROP COLUMN PubType, DROP COLUMN PubKey;

-- DROP TABLE Article;
CREATE TABLE Article (PubID INT NOT NULL, Title varchar (500), Journal varchar (255), Volume varchar (255), Number varchar (255), Pages varchar (255));

# Sanity Checks (tempArticle - No Records with multiple data of same PubID observed)
-- SELECT DISTINCT PubID FROM tempArticle WHERE Field LIKE 'journal' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different Pages records
-- SELECT DISTINCT PubID FROM tempArticle WHERE Field LIKE 'volume' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different volume records
-- SELECT DISTINCT PubID FROM tempArticle WHERE Field LIKE 'number' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different number records
-- SELECT DISTINCT PubID FROM tempArticle WHERE Field LIKE 'pages' GROUP BY PubID HAVING count(Field) > 1 LIMIT 4; # No multiple different Pages records

INSERT INTO Article (SELECT PubID, 
  MAX(case when Field = 'title' then Entry else NULL end) Title,  
  MAX(case when Field = 'journal' then Entry else NULL end) Journal,
  MAX(case when Field = 'volume' then Entry else NULL end) Volume,
  MAX(case when Field = 'number' then Entry else NULL end) Number,
  MAX(case when Field = 'pages' then Entry else NULL end) Pages
FROM (SELECT ROW_NUMBER() OVER(PARTITION BY PubID, Field ORDER BY PubID) AS Counter, PubID, Field, Entry FROM tempArticle
GROUP BY PubID, Field, Entry) a
GROUP BY PubID, Counter);

# Drop Title column and add Foreign Key constraints
ALTER TABLE Article DROP COLUMN Title, ADD FOREIGN KEY (PubID) REFERENCES Publication(PubID);
-- select COUNT(DISTINCT PubID) from Article;

# Data Transformation Pipeline (Author Table)
-- DROP TABLE tempAuthor;
CREATE TABLE tempAuthor (PubKey varchar (255) NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));

# Insert into temp Author table after subsetting of desired data with author field
INSERT INTO tempAuthor (select * from (select * from RawData
WHERE (PubType LIKE 'article%' OR PubType LIKE 'book%' OR PubType LIKE 'incollection%' OR PubType LIKE 'inproceedings%' OR PubType LIKE 'proceedings%')
AND (PubKey LIKE 'conf%' OR PubKey LIKE 'books%' OR PubKey LIKE 'journals%')) subset
WHERE Field = 'author');
-- select * from tempAuthor;
ALTER TABLE tempAuthor DROP COLUMN Field, DROP COLUMN PubType;

# Clone another tempAuthor2 Table
-- DROP TABLE tempAuthor2;
CREATE TABLE tempAuthor2 AS SELECT * FROM tempAuthor;
ALTER TABLE tempAuthor2 DROP COLUMN PubKey;
-- select * from tempAuthor2;

# Create a temp AuthorHomepage Table with HomepageID
-- DROP TABLE tempAuthorHomepage;
CREATE TABLE tempAuthorHomepage (PubKey varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, Field varchar (255), Entry varchar (500), PubType varchar (255));

# Insert into temp AuthorHomepage table after subsetting of desired data with author field
INSERT INTO tempAuthorHomepage (select * from (select * from RawData
WHERE PubType LIKE 'www%'
AND (PubKey LIKE 'homepages%')) subset
WHERE Field = 'author');
-- select * from tempAuthorHomepage;
ALTER TABLE tempAuthorHomepage DROP COLUMN Field, DROP COLUMN PubType;

# Join author names to have their respective HomepageIDs
-- DROP TABLE Author_copy;
CREATE TABLE Author_copy (Name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, HomePageID varchar(255));
INSERT INTO Author_copy (SELECT TA.Entry, TAH.PubKey FROM tempAuthor2 TA LEFT JOIN tempAuthorHomepage TAH ON TA.Entry = TAH.Entry);
-- select * from Author_copy;

# Remove duplicated HomePageID from Author_copy
-- DROP TABLE Author_copy2;
CREATE TABLE Author_copy2 (Name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, HomePageID varchar(255));
INSERT INTO Author_copy2 (SELECT NAME, HomePageID FROM Author_copy GROUP BY NAME, HomePageID);
-- select * from Author_copy2;

# Create an impt Author Table that holds all records with their unique AuthorID. 
# Primary Key constraint implemented
-- DROP TABLE Author;
CREATE TABLE Author (AuthorID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, Name varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, HomePageID varchar (255));
INSERT INTO Author (SELECT ROW_NUMBER() OVER(ORDER BY HomePageID DESC), Name, HomePageID FROM Author_copy2 GROUP BY Name, HomePageID);
-- select * from Author;

# Data Transformation Pipeline (Authored Table)

# Join PubID and Author info together
-- DROP TABLE tempAuthored;
CREATE TABLE tempAuthored (PubID INT NOT NULL, PubKey varchar (255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, Entry varchar (500));
INSERT INTO tempAuthored (SELECT PubID, PL.PubKey, Entry FROM tempPubLink_copy PL INNER JOIN tempAuthor TA ON PL.PubKey = TA.PubKey);
-- select * from tempAuthored;

# Create an impt Authored Table that holds all records with their unique PubID and AuthorID. 
-- DROP TABLE Authored;
CREATE TABLE Authored (PubID INT NOT NULL, AuthorID INT NOT NULL);
INSERT INTO Authored (SELECT TA.PubID, A.AuthorID FROM tempAuthored TA LEFT JOIN Author A ON TA.Entry = A.Name ORDER BY PubID);

ALTER TABLE Authored ADD FOREIGN KEY (AuthorID) REFERENCES Author (AuthorID), ADD FOREIGN KEY (PubID) REFERENCES Publication (PubID);
-- select * from Authored;

DROP TABLE tempAuthor;
DROP TABLE tempAuthor2;
DROP TABLE tempAuthorHomepage;
DROP TABLE Author_copy;
DROP TABLE Author_copy2;
DROP TABLE tempAuthored;
DROP TABLE tempPublication2;

###########################################################################################################################

## Queries example
	
## Create Temp Count table for cross-reference between group members
-- DROP TABLE TempCountTable;
CREATE TABLE TempCountTable (TableName VARCHAR(25), RecordsCount INT, DistinctPubID INT, DistinctPubKey INT, DistinctAuthorID INT);
INSERT INTO TempCountTable 
VALUES 
('Publication', 	(SELECT COUNT(*) FROM Publication), 	(SELECT COUNT(DISTINCT PubID) FROM Publication),	(SELECT COUNT(DISTINCT PubKey) FROM Publication), NULL),
('Author', 			(SELECT COUNT(*) FROM Author), 			NULL,												NULL, (SELECT COUNT(DISTINCT AuthorID) FROM Author)),
('Authored', 		(SELECT COUNT(*) FROM Authored), 		(SELECT COUNT(DISTINCT PubID) FROM Authored),		NULL, (SELECT COUNT(DISTINCT AuthorID) FROM Authored)),
('Article', 		(SELECT COUNT(*) FROM Article), 		(SELECT COUNT(DISTINCT PubID) FROM Article),		NULL, NULL),
('Book', 			(SELECT COUNT(*) FROM Book), 			(SELECT COUNT(DISTINCT PubID) FROM Book),			NULL, NULL),
('Incollection', 	(SELECT COUNT(*) FROM Incollection), 	(SELECT COUNT(DISTINCT PubID) FROM Incollection),	NULL, NULL),
('Inproceedings', 	(SELECT COUNT(*) FROM Inproceedings),	(SELECT COUNT(DISTINCT PubID) FROM Inproceedings),	NULL, NULL),
('Proceedings', 	(SELECT COUNT(*) FROM Proceedings),		(SELECT COUNT(DISTINCT PubID) FROM Proceedings),	NULL, NULL);

select * FROM TempCountTable;

###########################################################################################################################

## Sample questions

## Q1. For each type of publication, count the total number of publications of that type between 2010-2019. 
## Your query should return a set of (publication-type, count) pairs. For example, (article, 20000), (inproceedings, 30000), ...

SELECT CONCAT(T2.PubType, ", ", T2.Count) AS 'Publication-type, Count'
FROM
(SELECT 	DISTINCT T.PubType AS 'PubType', 
			COUNT(DISTINCT T.PubID) 	AS 'Count'		
			FROM 
			(SELECT pub.*, 
					CASE 	WHEN p.PubID IS NOT NULL THEN 'Proceedings'
							WHEN i.PubID IS NOT NULL THEN 'Inproceedings'
							WHEN a.PubID IS NOT NULL THEN 'Article'				
							WHEN b.PubID IS NOT NULL THEN 'Book'		                
							WHEN ic.PubID IS NOT NULL THEN 'Incollection'
							END AS PubType
							FROM publication pub LEFT JOIN proceedings 	p 	ON pub.PubID = p.PubID 
												 LEFT JOIN inproceedings i 	ON pub.PubID = i.PubID
												 LEFT JOIN article 		a 	ON pub.PubID = a.PubID
												 LEFT JOIN book			b	ON pub.PubID = b.PubID 
												 LEFT JOIN incollection	ic	ON pub.PubID = ic.PubID
							WHERE pub.Year >= '2010' AND pub.Year <= '2019') AS T
							GROUP BY T.PubType) AS T2;

###########################################################################################################################                            

## Q2. Find all the conferences that have ever published more than 500 papers in one year. 
## Note that one conference may be held every year (e.g., KDD runs many years, and each year the conference has a number of papers).

SELECT DISTINCT ConferenceName FROM
(SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(t.PubKey, '/', -2), '/', 1) AS ConferenceName, 
t.Year, COUNT(DISTINCT ic.PubID) + COUNT(DISTINCT ip.PubID) AS num_of_papers
FROM (
    SELECT pub.PubKey, p.PubID, pub.Year
    FROM publication pub 
    LEFT JOIN proceedings p ON p.PubID = pub.PubID
    LEFT JOIN book b ON b.PubID = pub.PubID 
) AS t
LEFT JOIN inproceedings ip ON t.PubKey = ip.Crossref
LEFT JOIN incollection ic ON t.PubKey = ic.Crossref 
WHERE PubKey LIKE 'conf%'
GROUP BY ConferenceName, t.Year
HAVING num_of_papers > 500) a
ORDER BY ConferenceName;

########################################################################################################################### 

## Q3. List the name of the conferences such that it has ever been held in June, 
##     and the corresponding proceedings (in the year where the conference was held in June) contain more than 100 publications.

SELECT DISTINCT ConferenceName FROM (SELECT 
	SUBSTRING_INDEX(SUBSTRING_INDEX(pub.PubKey, '/', -2), '/', 1) AS ConferenceName,
    pub.Title AS ProceedingsName,
    COUNT(DISTINCT ip.PubID) AS NumOfInproceedings,
    pub.Year AS YearOfConference
FROM publication pub 
JOIN proceedings p ON pub.PubID = p.PubID 
JOIN inproceedings ip ON pub.PubKey = ip.Crossref 
WHERE pub.Title LIKE '%June%'
AND pub.PubKey LIKE 'conf%'
GROUP BY pub.PubID, pub.Title, ip.Crossref, pub.Year
HAVING NumOfInproceedings > 100) t
ORDER BY ConferenceName;

########################################################################################################################### 

## Q4. Find the names and number of publications for authors who have the earliest publication record in DBLP.

SELECT DISTINCT a1.Name  AS AuthorName, 
COUNT(DISTINCT p1.PubID) AS PublicationCount
FROM publication p1 	JOIN authored ad1 ON ad1.PubID = p1.PubID 
						JOIN author a1 ON ad1.AuthorID = a1.AuthorID
                        
WHERE a1.AuthorID IN 	(SELECT DISTINCT a.AuthorID 
						FROM publication p 	JOIN authored ad ON ad.PubID = p.PubID 
											JOIN author a ON ad.AuthorID = a.AuthorID
						WHERE p.year = (SELECT min(year) FROM publication))  
GROUP BY a1.AuthorID, a1.Name;

###########################################################################################################################
