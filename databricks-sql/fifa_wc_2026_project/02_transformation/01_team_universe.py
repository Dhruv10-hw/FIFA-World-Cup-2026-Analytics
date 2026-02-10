# Databricks notebook source
data = [

# ---------- GROUP A ----------
("Mexico","A","host",None,"CONCACAF",1),
("South Africa","A","confirmed",None,"CAF",0),
("Korea Republic","A","confirmed",None,"AFC",0),
("Denmark","A","playoff","DEN/MKD/CZE/IRL","UEFA",0),
("North Macedonia","A","playoff","DEN/MKD/CZE/IRL","UEFA",0),
("Czech Republic","A","playoff","DEN/MKD/CZE/IRL","UEFA",0),
("Republic of Ireland","A","playoff","DEN/MKD/CZE/IRL","UEFA",0),

# ---------- GROUP B ----------
("Canada","B","host",None,"CONCACAF",1),
("Qatar","B","confirmed",None,"AFC",0),
("Switzerland","B","confirmed",None,"UEFA",0),
("Italy","B","playoff","ITA/NIR/WAL/BIH","UEFA",0),
("Northern Ireland","B","playoff","ITA/NIR/WAL/BIH","UEFA",0),
("Wales","B","playoff","ITA/NIR/WAL/BIH","UEFA",0),
("Bosnia and Herzegovina","B","playoff","ITA/NIR/WAL/BIH","UEFA",0),

# ---------- GROUP C ----------
("Brazil","C","confirmed",None,"CONMEBOL",0),
("Morocco","C","confirmed",None,"CAF",0),
("Scotland","C","confirmed",None,"UEFA",0),
("Haiti","C","confirmed",None,"CONCACAF",0),

# ---------- GROUP D ----------
("United States","D","host",None,"CONCACAF",1),
("Paraguay","D","confirmed",None,"CONMEBOL",0),
("Australia","D","confirmed",None,"AFC",0),
("Turkey","D","playoff","TUR/ROU/SVK/KOS","UEFA",0),
("Romania","D","playoff","TUR/ROU/SVK/KOS","UEFA",0),
("Slovakia","D","playoff","TUR/ROU/SVK/KOS","UEFA",0),
("Kosovo","D","playoff","TUR/ROU/SVK/KOS","UEFA",0),

# ---------- GROUP E ----------
("Germany","E","confirmed",None,"UEFA",0),
("Ecuador","E","confirmed",None,"CONMEBOL",0),
("Côte d'Ivoire","E","confirmed",None,"CAF",0),
("Curaçao","E","confirmed",None,"CONCACAF",0),

# ---------- GROUP F ----------
("Netherlands","F","confirmed",None,"UEFA",0),
("Japan","F","confirmed",None,"AFC",0),
("Tunisia","F","confirmed",None,"CAF",0),
("Ukraine","F","playoff","UKR/SWE/POL/ALB","UEFA",0),
("Sweden","F","playoff","UKR/SWE/POL/ALB","UEFA",0),
("Poland","F","playoff","UKR/SWE/POL/ALB","UEFA",0),
("Albania","F","playoff","UKR/SWE/POL/ALB","UEFA",0),

# ---------- GROUP G ----------
("Belgium","G","confirmed",None,"UEFA",0),
("Egypt","G","confirmed",None,"CAF",0),
("IR Iran","G","confirmed",None,"AFC",0),
("New Zealand","G","confirmed",None,"OFC",0),

# ---------- GROUP H ----------
("Spain","H","confirmed",None,"UEFA",0),
("Uruguay","H","confirmed",None,"CONMEBOL",0),
("Saudi Arabia","H","confirmed",None,"AFC",0),
("Cabo Verde","H","confirmed",None,"CAF",0),

# ---------- GROUP I ----------
("France","I","confirmed",None,"UEFA",0),
("Senegal","I","confirmed",None,"CAF",0),
("Norway","I","confirmed",None,"UEFA",0),
("Bolivia","I","playoff","BOL/SUR/IRQ","CONMEBOL",0),
("Suriname","I","playoff","BOL/SUR/IRQ","CONCACAF",0),
("Iraq","I","playoff","BOL/SUR/IRQ","AFC",0),

# ---------- GROUP J ----------
("Argentina","J","confirmed",None,"CONMEBOL",0),
("Algeria","J","confirmed",None,"CAF",0),
("Austria","J","confirmed",None,"UEFA",0),
("Jordan","J","confirmed",None,"AFC",0),

# ---------- GROUP K ----------
("Portugal","K","confirmed",None,"UEFA",0),
("Colombia","K","confirmed",None,"CONMEBOL",0),
("Uzbekistan","K","confirmed",None,"AFC",0),
("New Caledonia","K","playoff","NCL/JAM/COD","OFC",0),
("Jamaica","K","playoff","NCL/JAM/COD","CONCACAF",0),
("DR Congo","K","playoff","NCL/JAM/COD","CAF",0),

# ---------- GROUP L ----------
("England","L","confirmed",None,"UEFA",0),
("Croatia","L","confirmed",None,"UEFA",0),
("Ghana","L","confirmed",None,"CAF",0),
("Panama","L","confirmed",None,"CONCACAF",0)

]

columns = [
    "team_name",
    "group_id",
    "slot_status",
    "qualification_path",
    "confederation",
    "host_flag"
]

team_universe_df = spark.createDataFrame(data, columns)
display(team_universe_df)


team_universe_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_wc2026_team_universe")
