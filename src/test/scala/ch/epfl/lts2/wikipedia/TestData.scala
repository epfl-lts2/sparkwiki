package ch.epfl.lts2.wikipedia

trait TestData {
  val sqlPage = "INSERT INTO `page` VALUES (10,0,'AccessibleComputing','',1,0,0.33167112649574004,'20180709171712','20180410125914',834079434,122,'wikitext',NULL),"+
                "(12,14,'Anarchism','',0,0,0.786172332974311,'20180730175243','20180730175339',851684166,188642,'wikitext',NULL),"+
                "(13,0,'AfghanistanHistory','',1,0,0.0621502865684687,'20180726011011','20180410125914',783865149,90,'wikitext',NULL),"+
                "(258,0,'AnchorageAlaska','',1,0,0.835559814083911,'20180801070358','20180410125916',783823547,85,'wikitext',NULL); ";
  val expectPage = "(10,0,'AccessibleComputing','',1,0,0.33167112649574004,'20180709171712','20180410125914',834079434,122,'wikitext',NULL),"+
                "(12,14,'Anarchism','',0,0,0.786172332974311,'20180730175243','20180730175339',851684166,188642,'wikitext',NULL),"+
                "(13,0,'AfghanistanHistory','',1,0,0.0621502865684687,'20180726011011','20180410125914',783865149,90,'wikitext',NULL),"+
                "(258,0,'AnchorageAlaska','',1,0,0.835559814083911,'20180801070358','20180410125916',783823547,85,'wikitext',NULL);"
    
  val sqlPageLong = "INSERT INTO `page` VALUES (45531,1,'Nutella','',0,0,0.991077012374138,'20180719182251','20180719183033',822603196,45774,'wikitext',NULL),"+
    "(45532,14,'Sicily','',0,0,0.225302804471276,'20180730215355','20180719183033',816829334,17508,'wikitext',NULL),"+
    "(45533,14,'Pseudorandom_number_generator','',0,0,0.15328301596761,'20180730215355','20180721083714',758633004,38067,'wikitext',NULL),"+
    "(45534,0,'Aldo_Rossi','',0,0,0.626400400536508,'20180729135853','20180727134319',839426882,21868,'wikitext',NULL),"+
    "(45535,0,'Alessi_(Italian_company)','',0,0,0.758433868883582,'20180714012338','20180714012338',850159824,9785,'wikitext',NULL),"+
    "(45536,1,'Hangul/Archive_1','',0,0,0.69190228236085,'20180725104745','20180725110615',715813089,128904,'wikitext',NULL),"+
    "(45537,0,'Ustad_Isa','',0,0,0.91296817354203,'20180725104745','20180725105318',797679342,2279,'wikitext',NULL),"+
    "(45538,0,'Mersenne_Twister','',0,0,0.289539291793051,'20180727131127','20180727131556',851758798,29213,'wikitext',NULL),"+
    "(45539,1,'Venus_(mythology)','',0,0,0.21354275589207397,'20180730215355','20180719183033',822111405,20769,'wikitext',NULL),"+
    "(45540,14,'Mersenne_Twister','',0,0,0.07296370907962671,'20180721082243','20180721083714',813229298,70956,'wikitext',NULL),"+
    "(45541,0,'Social_Darwinism','',0,0,0.7087919690728111,'20180729200203','20180729200203',852554169,39122,'wikitext',NULL),"+
    "(45542,1,'Social_Darwinism','',0,0,0.40206044577254,'20180730215355','20180721083714',849224912,49466,'wikitext',NULL),"+
    "(45543,1,'Amsterdam','',0,0,0.837731334229681,'20180730215355','20180729102829',851452750,63073,'wikitext',NULL),"+
    "(45545,1,'William_Ernest_Henley','',0,0,0.399182850866749,'20180730215355','20180719183033',812559971,4508,'wikitext',NULL),"+
    "(45546,0,'Refugees','',1,0,0.250334127107946,'20180725065111','20180410135248',769630669,70,'wikitext', NULL);"
  
  val expectPageLong = "(45531,1,'Nutella','',0,0,0.991077012374138,'20180719182251','20180719183033',822603196,45774,'wikitext',NULL),"+
    "(45532,14,'Sicily','',0,0,0.225302804471276,'20180730215355','20180719183033',816829334,17508,'wikitext',NULL),"+
    "(45533,14,'Pseudorandom_number_generator','',0,0,0.15328301596761,'20180730215355','20180721083714',758633004,38067,'wikitext',NULL),"+
    "(45534,0,'Aldo_Rossi','',0,0,0.626400400536508,'20180729135853','20180727134319',839426882,21868,'wikitext',NULL),"+
    "(45535,0,'Alessi_(Italian_company)','',0,0,0.758433868883582,'20180714012338','20180714012338',850159824,9785,'wikitext',NULL),"+
    "(45536,1,'Hangul/Archive_1','',0,0,0.69190228236085,'20180725104745','20180725110615',715813089,128904,'wikitext',NULL),"+
    "(45537,0,'Ustad_Isa','',0,0,0.91296817354203,'20180725104745','20180725105318',797679342,2279,'wikitext',NULL),"+
    "(45538,0,'Mersenne_Twister','',0,0,0.289539291793051,'20180727131127','20180727131556',851758798,29213,'wikitext',NULL),"+
    "(45539,1,'Venus_(mythology)','',0,0,0.21354275589207397,'20180730215355','20180719183033',822111405,20769,'wikitext',NULL),"+
    "(45540,14,'Mersenne_Twister','',0,0,0.07296370907962671,'20180721082243','20180721083714',813229298,70956,'wikitext',NULL),"+
    "(45541,0,'Social_Darwinism','',0,0,0.7087919690728111,'20180729200203','20180729200203',852554169,39122,'wikitext',NULL),"+
    "(45542,1,'Social_Darwinism','',0,0,0.40206044577254,'20180730215355','20180721083714',849224912,49466,'wikitext',NULL),"+
    "(45543,1,'Amsterdam','',0,0,0.837731334229681,'20180730215355','20180729102829',851452750,63073,'wikitext',NULL),"+
    "(45545,1,'William_Ernest_Henley','',0,0,0.399182850866749,'20180730215355','20180719183033',812559971,4508,'wikitext',NULL),"+
    "(45546,0,'Refugees','',1,0,0.250334127107946,'20180725065111','20180410135248',769630669,70,'wikitext', NULL);"

  val sqlPageTest = "INSERT INTO `page` VALUES (2,1,'Main_Page/Archive_3',0,0,0.230137635595,'20220602051933','20220602051932',373819,2575,'wikitext',NULL),"+
    "(3,0,'Jon_Harald_Søby',0,0,0.28238286483,'20180813135855','20211107055312',119096,166,'wikitext',NULL),"+
    "(7,0,'Helios89~testwiki',0,0,0.270216988752,'20150415212942','20150415212941',226361,65,'wikitext',NULL),"+
    "(8,0,'Ilyanep',0,0,0.226528387236,'20150602173418',NULL,2292,468,'wikitext',NULL),"+
    "(10,14,'Administrators',0,0,0.738010379441,'20200222211347','20200222211347',418321,1154,'wikitext',NULL),"+
    "(11,14,'About',0,0,0.1540784367,'20200722015244','20200722015224',442680,148,'wikitext',NULL),"+
    "(14,0,'Kahusi',0,0,0.041667912111,'20130406110536',NULL,59700,1099,'wikitext',NULL),"+
    "(17,3,'Ilyanep',0,0,0.460445165148,'20060211233251',NULL,501,687,'wikitext',NULL),"+
    "(18,14,'Aboutsite',0,1,0.247852777155,'20140523104802',NULL,37,69,'wikitext',NULL);"


  val sqlPageLinks = "INSERT INTO `pagelinks` VALUES (53942034,0,'1000_in_Japan',0),(53942079,0,'1000_in_Japan',0),(53942112,0,'1000_in_Japan',0),"+
    "(53942127,0,'1000_in_Japan',0),(53945851,0,'1000_in_Japan',0),(53946178,0,'1000_in_Japan',0),(53946194,0,'1000_in_Japan',0),"+
    "(53946212,0,'1000_in_Japan',0),(53946224,0,'1000_in_Japan',0),(53946269,0,'1000_in_Japan',0),(53946281,0,'1000_in_Japan',0),"+
    "(53946300,0,'1000_in_Japan',0),(53946318,0,'1000_in_Japan',0),(53946331,0,'1000_in_Japan',0),(53946342,0,'1000_in_Japan',0),"+
    "(53946349,0,'1000_in_Japan',0),(53946370,0,'1000_in_Japan',0),(53946392,0,'1000_in_Japan',0),(53946399,0,'1000_in_Japan',0),"+
    "(53946409,0,'1000_in_Japan',0),(53946582,0,'1000_in_Japan',0),(53946594,0,'1000_in_Japan',0),(53946624,0,'1000_in_Japan',0),"+
    "(53946638,0,'1000_in_Japan',0),(53946650,0,'1000_in_Japan',0),(53946679,0,'1000_in_Japan',0),(53946689,0,'1000_in_Japan',0),"+
    "(53946715,0,'1000_in_Japan',0),(53946726,0,'1000_in_Japan',0),(53946737,0,'1000_in_Japan',0),(53952923,0,'1000_in_Japan',0),"+
    "(53952958,0,'1000_in_Japan',0),(53952993,0,'1000_in_Japan',0),(53953002,0,'1000_in_Japan',0),(53953013,0,'1000_in_Japan',0),"+
    "(53953021,0,'1000_in_Japan',0),(53953037,0,'1000_in_Japan',0),(53986322,0,'1000_in_Japan',0),(53986345,0,'1000_in_Japan',0),"+
    "(53986353,0,'1000_in_Japan',0),(53986373,0,'1000_in_Japan',0),(53986397,0,'1000_in_Japan',0),(53989891,0,'1000_in_Japan',0),"+
    "(53989911,0,'1000_in_Japan',0);"
  
  val sqlRedirect = "INSERT INTO `redirect` VALUES (10,0,'Computer_accessibility','',''),(13,0,'History_of_Afghanistan','',''),(14,0,'Geography_of_Afghanistan','',''),"+
    "(15,0,'Demographics_of_Afghanistan','',''),(18,0,'Communications_in_Afghanistan','',''),(19,0,'Transport_in_Afghanistan','',''),"+
    "(20,0,'Afghan_Armed_Forces','',''),(21,0,'Foreign_relations_of_Afghanistan','',''),(23,0,'Assistive_technology','',''),"+
    "(24,0,'Amoeba','',''),(27,0,'History_of_Albania','','');"
  
  val sqlCategory = "INSERT INTO `category` VALUES (388194,'Colombian_death_metal_musical_groups',2,0,0),(388195,'Museums_in_Leicestershire',22,5,0),"+
    "(388196,'Culture_in_Leicestershire',23,7,0),(388198,'Sports_in_Norfolk,_Virginia',20,6,0),(388199,'Portuguese_death_metal_musical_groups',2,0,0),"+
    "(388204,'Sports_venues_in_Norfolk,_Virginia',14,0,0),(388205,'Swiss_death_metal_musical_groups',5,0,0),(388206,'Peruvian_death_metal_musical_groups',2,0,0),"+
    "(388207,'Peruvian_black_metal_musical_groups',2,0,0),(388208,'Peruvian_heavy_metal_musical_groups',2,2,0),(388210,'West_Michigan_Whitecaps_players',153,0,0),"+
    "(388211,'Erie_SeaWolves_players',247,0,0),(388215,'Works_by_Alan_Garner',2,2,0),(388219,'Belarusian_heavy_metal_musical_groups',7,0,0),"+
    "(388220,'Short_stories_by_Alan_Garner',1,0,0),(388221,'Museums_in_Cheshire',26,5,0),(388222,'Culture_in_Cheshire',14,8,0);"
  
  val sqlCatLink = "INSERT INTO `categorylinks` VALUES (6533,'English_male_short_story_writers','U9??9)AM^F^D-7)','2015-08-12 01:42:44','Williams, Charles','uca-default-u-kn','page'),"+
    "(6533,'EngvarB_from_August_2014','U9??9)AM^F^D-7)K?1M^C^F-','2014-08-26 01:17:19','Williams, Charles','uca-default-u-kn','page'),"+
    "(6533,'Inklings','U9??9)AM^F^D-7)K?1M^C^F-7)K?1M^DU9??9)AM','2007-06-20 18:59:31','Williams, Charles','uca-default-u-kn','page'),"+
    "(6533,'Mythopoeic_writers','U9??9)AM^F^D-7)K?1M^C^F-7)K?1M^DU9??9)AM^D','2007-06-20 18:59:31','Williams, Charles','uca-default-u-kn','page'),"+
    "(6533,'Oxford_University_Press_people','U9??9)AM^F^D-7)K?1M^C^F-7)K?1M^DU9??','2018-07-23 16:46:06','Williams, Charles','uca-default-u-kn','page'),"+
    "(6533,'Pages_using_citations_with_format_and_no_URL','U9??9)AM^F^D-7)K?1M^C^F-7)K?1M^DU9','2013-04-05 16:17:50','Williams, Charles','uca-default-u-kn','page'),"+
    "(6533,'People_educated_at_St_Albans_School,_Hertfordshire','U9??','2011-08-24 20:20:49','Williams, Charles','uca-default-u-kn','page');"
    
  val pageCountLegacy = """en.z 16th_World_Science_Fiction_Convention 1 D1
en.z 16th_World_Scout_Jamboree 7 B4K1S2
en.z 16th_amendment 2 C1P1
#comment line should be skipped
en.z Category:16th_ammendment 1 J1
en.z 16th_and_17th_Republican_People's_Party_Extraordinary_Conventions 1 H1
en.z 16th_arrondissement 3 O1Q1S1
en.z 16th_arrondissement_of_Marseille 22 B1E1K1L2M3N3P2Q3R2S1T1V1W1
en.z 16th_arrondissement_of_Paris 188 A6B7C7D8E4F3G5H2I4J8K6L3M12N32O9P10Q6R13S14T7U8V5W3X6
en.z 16th_century 258 A9B13C9D15E7F14G9H8I8J9K8L4M9N18O9P15Q17R12S10T12U7V15W15X6
en.z 16th_century_BC 24 A2B2C1D1E2F2I1L1M1N3Q1R1S2T1U1V1X1
en.z 16th_century_england 2 Q1S1
en.z 16th_century_in_Canada 9 H1J2L1N1P1Q2R1
en.z 16th_century_in_North_American_history 5 C2F1G1Q1
en.z 16th_century_in_South_Africa 2 D1O1
en.z 16th_century_in_Wales 6 N1P3T1X1
en.z Book:16th_century_in_literature 14 B2F1H2I1L1N1Q3R2T1
en.z 16th_century_in_poetry 5 I2K1L1X1
en.z 16th_district_of_Budapest 5 B1M1P1Q1X1
en.z 16th_meridian_east 7 C2G1H1J1L1P1
en.z 16th_note 1 M1
en.z 16th_parallel_north 17 B1C1E2I1M1N1O3P2Q1R1S1W2"""

  val pageViews = """en.wikipedia ABC_(The_Jackson_5_album) 1504608 desktop 61 A3B5C4D1E2F1I5K2L1M1N3O1P3Q3R5S4T3U3V5W2X4
en.wikipedia ABC_(The_Jackson_5_album) 1504608 mobile-web 66 A4B4C1D1E4F1G2H2K4L13N1O3P4Q3S2T5U4V3W1X4
en.wikipedia ABC_(The_Jackson_5_song) 1689944 desktop 88 A2B7C3D7E3H3I3J2K1L1M3N6O1P1Q4R5S10T6U3V9W4X4
en.wikipedia ABC_(The_Jackson_5_song) 1689944 mobile-web 86 A4B3C4D1E1F2G2H1I3J2K4L1O6P2Q5R8S2T8U14V5W3X5
en.wikipedia ABC_(U.S._TV_network) 62027 mobile-web 1 U1
en.wikipedia ABC_(United_States) 62027 desktop 2 R1V1
en.wikipedia ABC_(band) 207485 desktop 225 A12B9C5D5E7F4G6H3I12J8K10L5M8N11O8P7Q20R11S11T8U16V14W16X9
en.wikipedia ABC_(band) 207485 mobile-web 152 A7B6C5D1E7F6G4H1I6J6K3L5M6N3O5P5Q11R9S8T7U9V12W13X7
en.wikipedia ABC_(band) null mobile-app 13 E1F1G1J1K4M1R1U1V1X1
en.wikipedia ABC_(broadcasting) 2166 desktop 2 A1V1
en.wikipedia ABC_(channel) 2166 desktop 1 P1
en.wikipedia ABC_(computer_virus) 2295389 desktop 1 W1
en.wikipedia ABC_(computer_virus) 2295389 mobile-web 3 B1E1P1
en.wikipedia ABC_(disambiguation) 2166 desktop 4 C1H1R1T1
en.wikipedia ABC_(disambiguation) 2166 mobile-web 3 M2V1
en.wikipedia ABC_(food) 46652775 desktop 19 B2E1F3I1J1K1M1N2P1S1T2U1X2
en.wikipedia ABC_(food) 46652775 mobile-web 21 A1C1G2H1J1L2M2O1P2R2S1T1U1V1X2
en.wikipedia ABC_(food) null mobile-app 1 X1
en.wikipedia ABC_(magazine) 51257333 mobile-web 1 T1
en.wikipedia ABC_(medicine) 3524059 desktop 122 A4B3C2D4E1F2G7H4I5J8K9L5M3N1O6P5Q9R6S9T11U4V5W1X8
en.wikipedia ABC_(medicine) 3524059 mobile-web 236 A10B7C15D7E10F14G5H12I12J15K9L6M7N12O6P11Q10R14S17T7U3V9W8X10
en.wikipedia ABC_(medicine) null mobile-app 5 G1O1U3
en.wikipedia ABC_(newspaper) 401572 desktop 35 A3D2E1G2H1I1J2K2L1O2P5Q1R2S1T2U2V2W1X2
en.wikipedia ABC_(newspaper) 401572 mobile-web 20 A1B1C1D1E1F1G1H1J2K1O1S2U1V2X3
en.wikipedia ABC_(newspaper) null mobile-app 3 V3
en.wikipedia ABC_(programming_language) 147585 desktop 83 A2B3C5D7E3F4H4I2J2K3L4M4N5O1P3Q6R1S3T8U5W4X4
en.wikipedia ABC_(programming_language) 147585 mobile-web 21 A2B2C1D1H2J1L1N1P2Q1S1T1U1V2W1X1
en.wikipedia ABC_(programming_language) null mobile-app 2 G1P1
en.wikipedia ABC_(song) 22554674 desktop 3 L2Q1
en.wikipedia ABC_(stream_cipher) 6339424 desktop 1 O1
en.wikipedia ABC_10_News 1636836 desktop 1 R1
en.wikipedia ABC_10_News 55232541 desktop 1 R1
en.wikipedia ABC_12 6849083 desktop 1 Q1
en.wikipedia ABC_13 6849095 desktop 4 B1E1I1Q1
en.wikipedia ABC_1600 30624465 desktop 3 I1K1T1
en.wikipedia ABC_1600 30624465 mobile-web 2 J2
en.wikipedia ABC_20/20 422220 desktop 1 Q1
en.wikipedia ABC_20/20 422220 mobile-web 2 E1X1
en.wikipedia ABC_2000 3879416 desktop 3 A1U2
en.wikipedia ABC_2000 3879416 mobile-web 1 O1
en.wikipedia ABC_2000_Today 3879416 desktop 14 A1B1F1H1O1Q1R3U2V1X2
en.wikipedia ABC_2000_Today 3879416 mobile-web 6 F1G1O1W1X2
en.wikipedia ABC_2000_Today null mobile-app 1 I1
"""

  val pageCountLegacy2 = List("en.z AfghanistanHistory 200 A20B20C20D20E20F20G20H20I20J20", "en.z Category:Anarchism 300 A60B60C60D60E60",
                        "en.z AccessibleComputing 120 A30B30C30D30", "en.z AnchorageAlaska 3000 E600F600G600H600J600")

  val pageViews2 = List("en.wikipedia ABC_(The_Jackson_5_song) 1689944 desktop 88 A2B7C3D7E3H3I3J2K1L1M3N6O1P1Q4R5S10T6U3V9W4X4",
    "en.wikipedia ABC_(The_Jackson_5_song) 1689944 mobile-web 86 A4B3C4D1E1F2G2H1I3J2K4L1O6P2Q5R8S2T8U14V5W3X5",
    "en.wikipedia ABC_(band) 207485 desktop 225 A12B9C5D5E7F4G6H3I12J8K10L5M8N11O8P7Q20R11S11T8U16V14W16X9",
    "en.wikipedia ABC_(band) 207485 mobile-web 152 A7B6C5D1E7F6G4H1I6J6K3L5M6N3O5P5Q11R9S8T7U9V12W13X7",
    "en.wikipedia ABC_(band) null mobile-app 13 E1F1G1J1K4M1R1U1V1X1",
    "en.wikipedia ABC_2000_Today 3879416 desktop 14 A1B1F1H1O1Q1R3U2V1X2",
    "en.wikipedia ABC_2000_Today 3879416 mobile-web 6 F1G1O1W1X2",
    "en.wikipedia ABC_2000_Today null mobile-app 1 I1")

  // 15 elts
  val langLinksFr = "INSERT INTO `langlinks` VALUES (34893,'fr','1640'),(47926106,'fr','1640 en France'),(2551557,'fr','1640 en arts plastiques'),(226379,'fr','1640 en littérature')," +
    "(5643747,'fr','1640 en musique classique'),(467170,'fr','1640 en science'),(35202,'fr','1641'),(47474227,'fr','1641 en France')," +
    "(10534777,'fr','1641 en arts plastiques'),(226378,'fr','1641 en littérature'),(342117,'fr','1641 en musique classique')," +
    "(473747,'fr','1641 en science'),(34587,'fr','1642'),(46591012,'fr','1642 en France'),(3267785,'fr','1642 en arts plastiques');"

  val langLinksFr2 = "INSERT INTO `langlinks` VALUES (34893,'fr','1640'),(47926106,'fr',''),(2551557,'fr','1640 en arts plastiques');"

  // 11 elts
  val langLinksEs = "INSERT INTO `langlinks` VALUES (20540019,'es','Algerian'),(24509264,'es','Algeripithecus'),(4524756,'es','Algermissen')," +
    "(995766,'es','Algernon Bertram Freeman Mitford'),(164993,'es','Algernon Blackwood'),(58090661,'es','Algernon Charles Swinburne')," +
    "(17017671,'es','Algernon Mayow Talmage'),(23728564,'es','Algernon Percy (diplomático)'),(7731133,'es','Algernon Percy, I conde de Beverley')," +
    "(603447,'es','Algernon Percy, IV duque de Northumberland'),(2234898,'es','Algernon Percy, VI duque de Northumberland');"
}