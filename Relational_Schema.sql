# Relational Schema (PubSchema) creation
DROP database project_db;
CREATE database project_db;
USE project_db;

CREATE TABLE Publication (PubID INT NOT NULL, PubKey varchar (255) UNIQUE, Title varchar (500), Year varchar (255), PRIMARY KEY(PubID, PubKey));
CREATE TABLE Book (PubID INT NOT NULL, ISBN varchar (255), Publisher varchar (255), FOREIGN KEY (PubID) REFERENCES Publication (PubID));
CREATE TABLE Incollection (PubID INT NOT NULL, BookTitle varchar (255), Crossref varchar (255), FOREIGN KEY (PubID) REFERENCES Publication (PubID), FOREIGN KEY (Crossref) REFERENCES Publication (PubKey));
CREATE TABLE Inproceedings (PubID INT NOT NULL, BookTitle varchar (255), Crossref varchar (255), FOREIGN KEY (PubID) REFERENCES Publication (PubID), FOREIGN KEY (Crossref) REFERENCES Publication (PubKey));
CREATE TABLE Proceedings (PubID INT NOT NULL, BookTitle varchar (500), Series varchar (500), FOREIGN KEY (PubID) REFERENCES Publication (PubID));
CREATE TABLE Article (PubID INT NOT NULL, Journal varchar (255), Volume varchar (255), Number varchar (255), Pages varchar (255), FOREIGN KEY (PubID) REFERENCES Publication (PubID));
CREATE TABLE Author (AuthorID INT NOT NULL PRIMARY KEY, Name varchar (255), HomePageID varchar (255));
CREATE TABLE Authored (PubID INT NOT NULL, AuthorID INT NOT NULL, FOREIGN KEY (AuthorID) REFERENCES Author (AuthorID), FOREIGN KEY (PubID) REFERENCES Publication (PubID));

## Steps to generate the PubSchema
## Database ---> Reverse Engineer ---> Relational Schema Model generated