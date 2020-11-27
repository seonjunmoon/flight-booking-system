CREATE TABLE Users (
    username varchar(20) PRIMARY KEY,
    hash VARBINARY(20),
    salt VARBINARY(20),
    balance int
);

CREATE TABLE Reservations (
    id int PRIMARY KEY,
    username VARCHAR(20),
    fid1 int,
    fid2 int,
    canceled int,
    paid int,
    date int,
    price int,
    FOREIGN KEY (username) REFERENCES Users(username)
);
