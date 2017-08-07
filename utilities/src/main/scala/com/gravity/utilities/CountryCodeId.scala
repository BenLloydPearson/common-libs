package com.gravity.utilities

import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.swagger.adapter.DefaultValueWriter
import com.gravity.utilities.grvjson._
import play.api.libs.json._

import scalaz.Value

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 6/9/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

//iso3116
/*
A1,"Anonymous Proxy"
A2,"Satellite Provider"
O1,"Other Country"
AD,"Andorra"
AE,"United Arab Emirates"
AF,"Afghanistan"
AG,"Antigua and Barbuda"
AI,"Anguilla"
AL,"Albania"
AM,"Armenia"
AO,"Angola"
AP,"Asia/Pacific Region"
AQ,"Antarctica"
AR,"Argentina"
AS,"American Samoa"
AT,"Austria"
AU,"Australia"
AW,"Aruba"
AX,"Aland Islands"
AZ,"Azerbaijan"
BA,"Bosnia and Herzegovina"
BB,"Barbados"
BD,"Bangladesh"
BE,"Belgium"
BF,"Burkina Faso"
BG,"Bulgaria"
BH,"Bahrain"
BI,"Burundi"
BJ,"Benin"
BL,"Saint Bartelemey"
BM,"Bermuda"
BN,"Brunei Darussalam"
BO,"Bolivia"
BQ,"Bonaire, Saint Eustatius and Saba"
BR,"Brazil"
BS,"Bahamas"
BT,"Bhutan"
BV,"Bouvet Island"
BW,"Botswana"
BY,"Belarus"
BZ,"Belize"
CA,"Canada"
CC,"Cocos (Keeling) Islands"
CD,"Congo, The Democratic Republic of the"
CF,"Central African Republic"
CG,"Congo"
CH,"Switzerland"
CI,"Cote d'Ivoire"
CK,"Cook Islands"
CL,"Chile"
CM,"Cameroon"
CN,"China"
CO,"Colombia"
CR,"Costa Rica"
CU,"Cuba"
CV,"Cape Verde"
CW,"Curacao"
CX,"Christmas Island"
CY,"Cyprus"
CZ,"Czech Republic"
DE,"Germany"
DJ,"Djibouti"
DK,"Denmark"
DM,"Dominica"
DO,"Dominican Republic"
DZ,"Algeria"
EC,"Ecuador"
EE,"Estonia"
EG,"Egypt"
EH,"Western Sahara"
ER,"Eritrea"
ES,"Spain"
ET,"Ethiopia"
EU,"Europe"
FI,"Finland"
FJ,"Fiji"
FK,"Falkland Islands (Malvinas)"
FM,"Micronesia, Federated States of"
FO,"Faroe Islands"
FR,"France"
GA,"Gabon"
GB,"United Kingdom"
GD,"Grenada"
GE,"Georgia"
GF,"French Guiana"
GG,"Guernsey"
GH,"Ghana"
GI,"Gibraltar"
GL,"Greenland"
GM,"Gambia"
GN,"Guinea"
GP,"Guadeloupe"
GQ,"Equatorial Guinea"
GR,"Greece"
GS,"South Georgia and the South Sandwich Islands"
GT,"Guatemala"
GU,"Guam"
GW,"Guinea-Bissau"
GY,"Guyana"
HK,"Hong Kong"
HM,"Heard Island and McDonald Islands"
HN,"Honduras"
HR,"Croatia"
HT,"Haiti"
HU,"Hungary"
ID,"Indonesia"
IE,"Ireland"
IL,"Israel"
IM,"Isle of Man"
IN,"India"
IO,"British Indian Ocean Territory"
IQ,"Iraq"
IR,"Iran, Islamic Republic of"
IS,"Iceland"
IT,"Italy"
JE,"Jersey"
JM,"Jamaica"
JO,"Jordan"
JP,"Japan"
KE,"Kenya"
KG,"Kyrgyzstan"
KH,"Cambodia"
KI,"Kiribati"
KM,"Comoros"
KN,"Saint Kitts and Nevis"
KP,"Korea, Democratic People's Republic of"
KR,"Korea, Republic of"
KW,"Kuwait"
KY,"Cayman Islands"
KZ,"Kazakhstan"
LA,"Lao People's Democratic Republic"
LB,"Lebanon"
LC,"Saint Lucia"
LI,"Liechtenstein"
LK,"Sri Lanka"
LR,"Liberia"
LS,"Lesotho"
LT,"Lithuania"
LU,"Luxembourg"
LV,"Latvia"
LY,"Libyan Arab Jamahiriya"
MA,"Morocco"
MC,"Monaco"
MD,"Moldova, Republic of"
ME,"Montenegro"
MF,"Saint Martin"
MG,"Madagascar"
MH,"Marshall Islands"
MK,"Macedonia"
ML,"Mali"
MM,"Myanmar"
MN,"Mongolia"
MO,"Macao"
MP,"Northern Mariana Islands"
MQ,"Martinique"
MR,"Mauritania"
MS,"Montserrat"
MT,"Malta"
MU,"Mauritius"
MV,"Maldives"
MW,"Malawi"
MX,"Mexico"
MY,"Malaysia"
MZ,"Mozambique"
NA,"Namibia"
NC,"New Caledonia"
NE,"Niger"
NF,"Norfolk Island"
NG,"Nigeria"
NI,"Nicaragua"
NL,"Netherlands"
NO,"Norway"
NP,"Nepal"
NR,"Nauru"
NU,"Niue"
NZ,"New Zealand"
OM,"Oman"
PA,"Panama"
PE,"Peru"
PF,"French Polynesia"
PG,"Papua New Guinea"
PH,"Philippines"
PK,"Pakistan"
PL,"Poland"
PM,"Saint Pierre and Miquelon"
PN,"Pitcairn"
PR,"Puerto Rico"
PS,"Palestinian Territory"
PT,"Portugal"
PW,"Palau"
PY,"Paraguay"
QA,"Qatar"
RE,"Reunion"
RO,"Romania"
RS,"Serbia"
RU,"Russian Federation"
RW,"Rwanda"
SA,"Saudi Arabia"
SB,"Solomon Islands"
SC,"Seychelles"
SD,"Sudan"
SE,"Sweden"
SG,"Singapore"
SH,"Saint Helena"
SI,"Slovenia"
SJ,"Svalbard and Jan Mayen"
SK,"Slovakia"
SL,"Sierra Leone"
SM,"San Marino"
SN,"Senegal"
SO,"Somalia"
SR,"Suriname"
SS,"South Sudan"
ST,"Sao Tome and Principe"
SV,"El Salvador"
SX,"Sint Maarten"
SY,"Syrian Arab Republic"
SZ,"Swaziland"
TC,"Turks and Caicos Islands"
TD,"Chad"
TF,"French Southern Territories"
TG,"Togo"
TH,"Thailand"
TJ,"Tajikistan"
TK,"Tokelau"
TL,"Timor-Leste"
TM,"Turkmenistan"
TN,"Tunisia"
TO,"Tonga"
TR,"Turkey"
TT,"Trinidad and Tobago"
TV,"Tuvalu"
TW,"Taiwan"
TZ,"Tanzania, United Republic of"
UA,"Ukraine"
UG,"Uganda"
UM,"United States Minor Outlying Islands"
US,"United States"
UY,"Uruguay"
UZ,"Uzbekistan"
VA,"Holy See (Vatican City State)"
VC,"Saint Vincent and the Grenadines"
VE,"Venezuela"
VG,"Virgin Islands, British"
VI,"Virgin Islands, U.S."
VN,"Vietnam"
VU,"Vanuatu"
WF,"Wallis and Futuna"
WS,"Samoa"
YE,"Yemen"
YT,"Mayotte"
ZA,"South Africa"
ZM,"Zambia"
ZW,"Zimbabwe"
 */

@SerialVersionUID(0l)
object CountryCodeId extends GrvEnum[Int] {
  case class Type(i: Int, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Int, name: String): CountryCodeId.Type = Type(id, name)

  override def defaultValue: CountryCodeId.Type = unknown

  val unknown: Type = Value(0, "unknown") //you can read
  val A1: Type = Value(1	, "A1") //	Anonymous Proxy
  val A2: Type = Value(2	, "A2") //	Satellite Provider
  val O1: Type = Value(3	, "O1") //	Other Country
  val AD: Type = Value(4	, "AD") //	Andorra
  val AE: Type = Value(5	, "AE") //	United Arab Emirates
  val AF: Type = Value(6	, "AF") //	Afghanistan
  val AG: Type = Value(7	, "AG") //	Antigua and Barbuda
  val AI: Type = Value(8	, "AI") //	Anguilla
  val AL: Type = Value(9	, "AL") //	Albania
  val AM: Type = Value(10	, "AM") //	Armenia
  val AO: Type = Value(11	, "AO") //	Angola
  val AP: Type = Value(12	, "AP") //	Asia/Pacific Region
  val AQ: Type = Value(13	, "AQ") //	Antarctica
  val AR: Type = Value(14	, "AR") //	Argentina
  val AS: Type = Value(15	, "AS") //	American Samoa
  val AT: Type = Value(16	, "AT") //	Austria
  val AU: Type = Value(17	, "AU") //	Australia
  val AW: Type = Value(18	, "AW") //	Aruba
  val AX: Type = Value(19	, "AX") //	Aland Islands
  val AZ: Type = Value(20	, "AZ") //	Azerbaijan
  val BA: Type = Value(21	, "BA") //	Bosnia and Herzegovina
  val BB: Type = Value(22	, "BB") //	Barbados
  val BD: Type = Value(23	, "BD") //	Bangladesh
  val BE: Type = Value(24	, "BE") //	Belgium
  val BF: Type = Value(25	, "BF") //	Burkina Faso
  val BG: Type = Value(26	, "BG") //	Bulgaria
  val BH: Type = Value(27	, "BH") //	Bahrain
  val BI: Type = Value(28	, "BI") //	Burundi
  val BJ: Type = Value(29	, "BJ") //	Benin
  val BL: Type = Value(30	, "BL") //	Saint Bartelemey
  val BM: Type = Value(31	, "BM") //	Bermuda
  val BN: Type = Value(32	, "BN") //	Brunei Darussalam
  val BO: Type = Value(33	, "BO") //	Bolivia
  val BQ: Type = Value(34	, "BQ") //	Bonaire, Saint Eustatius and Saba
  val BR: Type = Value(35	, "BR") //	Brazil
  val BS: Type = Value(36	, "BS") //	Bahamas
  val BT: Type = Value(37	, "BT") //	Bhutan
  val BV: Type = Value(38	, "BV") //	Bouvet Island
  val BW: Type = Value(39	, "BW") //	Botswana
  val BY: Type = Value(40	, "BY") //	Belarus
  val BZ: Type = Value(41	, "BZ") //	Belize
  val CA: Type = Value(42	, "CA") //	Canada
  val CC: Type = Value(43	, "CC") //	Cocos (Keeling) Islands
  val CD: Type = Value(44	, "CD") //	Congo, The Democratic Republic of the
  val CF: Type = Value(45	, "CF") //	Central African Republic
  val CG: Type = Value(46	, "CG") //	Congo
  val CH: Type = Value(47	, "CH") //	Switzerland
  val CI: Type = Value(48	, "CI") //	Cote d'Ivoire
  val CK: Type = Value(49	, "CK") //	Cook Islands
  val CL: Type = Value(50	, "CL") //	Chile
  val CM: Type = Value(51	, "CM") //	Cameroon
  val CN: Type = Value(52	, "CN") //	China
  val CO: Type = Value(53	, "CO") //	Colombia
  val CR: Type = Value(54	, "CR") //	Costa Rica
  val CU: Type = Value(55	, "CU") //	Cuba
  val CV: Type = Value(56	, "CV") //	Cape Verde
  val CW: Type = Value(57	, "CW") //	Curacao
  val CX: Type = Value(58	, "CX") //	Christmas Island
  val CY: Type = Value(59	, "CY") //	Cyprus
  val CZ: Type = Value(60	, "CZ") //	Czech Republic
  val DE: Type = Value(61	, "DE") //	Germany
  val DJ: Type = Value(62	, "DJ") //	Djibouti
  val DK: Type = Value(63	, "DK") //	Denmark
  val DM: Type = Value(64	, "DM") //	Dominica
  val DO: Type = Value(65	, "DO") //	Dominican Republic
  val DZ: Type = Value(66	, "DZ") //	Algeria
  val EC: Type = Value(67	, "EC") //	Ecuador
  val EE: Type = Value(68	, "EE") //	Estonia
  val EG: Type = Value(69	, "EG") //	Egypt
  val EH: Type = Value(70	, "EH") //	Western Sahara
  val ER: Type = Value(71	, "ER") //	Eritrea
  val ES: Type = Value(72	, "ES") //	Spain
  val ET: Type = Value(73	, "ET") //	Ethiopia
  val EU: Type = Value(74	, "EU") //	Europe
  val FI: Type = Value(75	, "FI") //	Finland
  val FJ: Type = Value(76	, "FJ") //	Fiji
  val FK: Type = Value(77	, "FK") //	Falkland Islands (Malvinas)
  val FM: Type = Value(78	, "FM") //	Micronesia, Federated States of
  val FO: Type = Value(79	, "FO") //	Faroe Islands
  val FR: Type = Value(80	, "FR") //	France
  val GA: Type = Value(81	, "GA") //	Gabon
  val GB: Type = Value(82	, "GB") //	United Kingdom
  val GD: Type = Value(83	, "GD") //	Grenada
  val GE: Type = Value(84	, "GE") //	Georgia
  val GF: Type = Value(85	, "GF") //	French Guiana
  val GG: Type = Value(86	, "GG") //	Guernsey
  val GH: Type = Value(87	, "GH") //	Ghana
  val GI: Type = Value(88	, "GI") //	Gibraltar
  val GL: Type = Value(89	, "GL") //	Greenland
  val GM: Type = Value(90	, "GM") //	Gambia
  val GN: Type = Value(91	, "GN") //	Guinea
  val GP: Type = Value(92	, "GP") //	Guadeloupe
  val GQ: Type = Value(93	, "GQ") //	Equatorial Guinea
  val GR: Type = Value(94	, "GR") //	Greece
  val GS: Type = Value(95	, "GS") //	South Georgia and the South Sandwich Islands
  val GT: Type = Value(96	, "GT") //	Guatemala
  val GU: Type = Value(97	, "GU") //	Guam
  val GW: Type = Value(98	, "GW") //	Guinea-Bissau
  val GY: Type = Value(99	, "GY") //	Guyana
  val HK: Type = Value(100	, "HK") //	Hong Kong
  val HM: Type = Value(101	, "HM") //	Heard Island and McDonald Islands
  val HN: Type = Value(102	, "HN") //	Honduras
  val HR: Type = Value(103	, "HR") //	Croatia
  val HT: Type = Value(104	, "HT") //	Haiti
  val HU: Type = Value(105	, "HU") //	Hungary
  val ID: Type = Value(106	, "ID") //	Indonesia
  val IE: Type = Value(107	, "IE") //	Ireland
  val IL: Type = Value(108	, "IL") //	Israel
  val IM: Type = Value(109	, "IM") //	Isle of Man
  val IN: Type = Value(110	, "IN") //	India
  val IO: Type = Value(111	, "IO") //	British Indian Ocean Territory
  val IQ: Type = Value(112	, "IQ") //	Iraq
  val IR: Type = Value(113	, "IR") //	Iran, Islamic Republic of
  val IS: Type = Value(114	, "IS") //	Iceland
  val IT: Type = Value(115	, "IT") //	Italy
  val JE: Type = Value(116	, "JE") //	Jersey
  val JM: Type = Value(117	, "JM") //	Jamaica
  val JO: Type = Value(118	, "JO") //	Jordan
  val JP: Type = Value(119	, "JP") //	Japan
  val KE: Type = Value(120	, "KE") //	Kenya
  val KG: Type = Value(121	, "KG") //	Kyrgyzstan
  val KH: Type = Value(122	, "KH") //	Cambodia
  val KI: Type = Value(123	, "KI") //	Kiribati
  val KM: Type = Value(124	, "KM") //	Comoros
  val KN: Type = Value(125	, "KN") //	Saint Kitts and Nevis
  val KP: Type = Value(126	, "KP") //	Korea, Democratic People's Republic of
  val KR: Type = Value(127	, "KR") //	Korea, Republic of
  val KW: Type = Value(128	, "KW") //	Kuwait
  val KY: Type = Value(129	, "KY") //	Cayman Islands
  val KZ: Type = Value(130	, "KZ") //	Kazakhstan
  val LA: Type = Value(131	, "LA") //	Lao People's Democratic Republic
  val LB: Type = Value(132	, "LB") //	Lebanon
  val LC: Type = Value(133	, "LC") //	Saint Lucia
  val LI: Type = Value(134	, "LI") //	Liechtenstein
  val LK: Type = Value(135	, "LK") //	Sri Lanka
  val LR: Type = Value(136	, "LR") //	Liberia
  val LS: Type = Value(137	, "LS") //	Lesotho
  val LT: Type = Value(138	, "LT") //	Lithuania
  val LU: Type = Value(139	, "LU") //	Luxembourg
  val LV: Type = Value(140	, "LV") //	Latvia
  val LY: Type = Value(141	, "LY") //	Libyan Arab Jamahiriya
  val MA: Type = Value(142	, "MA") //	Morocco
  val MC: Type = Value(143	, "MC") //	Monaco
  val MD: Type = Value(144	, "MD") //	Moldova, Republic of
  val ME: Type = Value(145	, "ME") //	Montenegro
  val MF: Type = Value(146	, "MF") //	Saint Martin
  val MG: Type = Value(147	, "MG") //	Madagascar
  val MH: Type = Value(148	, "MH") //	Marshall Islands
  val MK: Type = Value(149	, "MK") //	Macedonia
  val ML: Type = Value(150	, "ML") //	Mali
  val MM: Type = Value(151	, "MM") //	Myanmar
  val MN: Type = Value(152	, "MN") //	Mongolia
  val MO: Type = Value(153	, "MO") //	Macao
  val MP: Type = Value(154	, "MP") //	Northern Mariana Islands
  val MQ: Type = Value(155	, "MQ") //	Martinique
  val MR: Type = Value(156	, "MR") //	Mauritania
  val MS: Type = Value(157	, "MS") //	Montserrat
  val MT: Type = Value(158	, "MT") //	Malta
  val MU: Type = Value(159	, "MU") //	Mauritius
  val MV: Type = Value(160	, "MV") //	Maldives
  val MW: Type = Value(161	, "MW") //	Malawi
  val MX: Type = Value(162	, "MX") //	Mexico
  val MY: Type = Value(163	, "MY") //	Malaysia
  val MZ: Type = Value(164	, "MZ") //	Mozambique
  val NA: Type = Value(165	, "NA") //	Namibia
  val NC: Type = Value(166	, "NC") //	New Caledonia
  val NE: Type = Value(167	, "NE") //	Niger
  val NF: Type = Value(168	, "NF") //	Norfolk Island
  val NG: Type = Value(169	, "NG") //	Nigeria
  val NI: Type = Value(170	, "NI") //	Nicaragua
  val NL: Type = Value(171	, "NL") //	Netherlands
  val NO: Type = Value(172	, "NO") //	Norway
  val NP: Type = Value(173	, "NP") //	Nepal
  val NR: Type = Value(174	, "NR") //	Nauru
  val NU: Type = Value(175	, "NU") //	Niue
  val NZ: Type = Value(176	, "NZ") //	New Zealand
  val OM: Type = Value(177	, "OM") //	Oman
  val PA: Type = Value(178	, "PA") //	Panama
  val PE: Type = Value(179	, "PE") //	Peru
  val PF: Type = Value(180	, "PF") //	French Polynesia
  val PG: Type = Value(181	, "PG") //	Papua New Guinea
  val PH: Type = Value(182	, "PH") //	Philippines
  val PK: Type = Value(183	, "PK") //	Pakistan
  val PL: Type = Value(184	, "PL") //	Poland
  val PM: Type = Value(185	, "PM") //	Saint Pierre and Miquelon
  val PN: Type = Value(186	, "PN") //	Pitcairn
  val PR: Type = Value(187	, "PR") //	Puerto Rico
  val PS: Type = Value(188	, "PS") //	Palestinian Territory
  val PT: Type = Value(189	, "PT") //	Portugal
  val PW: Type = Value(190	, "PW") //	Palau
  val PY: Type = Value(191	, "PY") //	Paraguay
  val QA: Type = Value(192	, "QA") //	Qatar
  val RE: Type = Value(193	, "RE") //	Reunion
  val RO: Type = Value(194	, "RO") //	Romania
  val RS: Type = Value(195	, "RS") //	Serbia
  val RU: Type = Value(196	, "RU") //	Russian Federation
  val RW: Type = Value(197	, "RW") //	Rwanda
  val SA: Type = Value(198	, "SA") //	Saudi Arabia
  val SB: Type = Value(199	, "SB") //	Solomon Islands
  val SC: Type = Value(200	, "SC") //	Seychelles
  val SD: Type = Value(201	, "SD") //	Sudan
  val SE: Type = Value(202	, "SE") //	Sweden
  val SG: Type = Value(203	, "SG") //	Singapore
  val SH: Type = Value(204	, "SH") //	Saint Helena
  val SI: Type = Value(205	, "SI") //	Slovenia
  val SJ: Type = Value(206	, "SJ") //	Svalbard and Jan Mayen
  val SK: Type = Value(207	, "SK") //	Slovakia
  val SL: Type = Value(208	, "SL") //	Sierra Leone
  val SM: Type = Value(209	, "SM") //	San Marino
  val SN: Type = Value(210	, "SN") //	Senegal
  val SO: Type = Value(211	, "SO") //	Somalia
  val SR: Type = Value(212	, "SR") //	Suriname
  val SS: Type = Value(213	, "SS") //	South Sudan
  val ST: Type = Value(214	, "ST") //	Sao Tome and Principe
  val SV: Type = Value(215	, "SV") //	El Salvador
  val SX: Type = Value(216	, "SX") //	Sint Maarten
  val SY: Type = Value(217	, "SY") //	Syrian Arab Republic
  val SZ: Type = Value(218	, "SZ") //	Swaziland
  val TC: Type = Value(219	, "TC") //	Turks and Caicos Islands
  val TD: Type = Value(220	, "TD") //	Chad
  val TF: Type = Value(221	, "TF") //	French Southern Territories
  val TG: Type = Value(222	, "TG") //	Togo
  val TH: Type = Value(223	, "TH") //	Thailand
  val TJ: Type = Value(224	, "TJ") //	Tajikistan
  val TK: Type = Value(225	, "TK") //	Tokelau
  val TL: Type = Value(226	, "TL") //	Timor-Leste
  val TM: Type = Value(227	, "TM") //	Turkmenistan
  val TN: Type = Value(228	, "TN") //	Tunisia
  val TO: Type = Value(229	, "TO") //	Tonga
  val TR: Type = Value(230	, "TR") //	Turkey
  val TT: Type = Value(231	, "TT") //	Trinidad and Tobago
  val TV: Type = Value(232	, "TV") //	Tuvalu
  val TW: Type = Value(233	, "TW") //	Taiwan
  val TZ: Type = Value(234	, "TZ") //	Tanzania, United Republic of
  val UA: Type = Value(235	, "UA") //	Ukraine
  val UG: Type = Value(236	, "UG") //	Uganda
  val UM: Type = Value(237	, "UM") //	United States Minor Outlying Islands
  val US: Type = Value(238	, "US") //	United States
  val UY: Type = Value(239	, "UY") //	Uruguay
  val UZ: Type = Value(240	, "UZ") //	Uzbekistan
  val VA: Type = Value(241	, "VA") //	Holy See (Vatican City State)
  val VC: Type = Value(242	, "VC") //	Saint Vincent and the Grenadines
  val VE: Type = Value(243	, "VE") //	Venezuela
  val VG: Type = Value(244	, "VG") //	Virgin Islands, British
  val VI: Type = Value(245	, "VI") //	Virgin Islands, U.S.
  val VN: Type = Value(246	, "VN") //	Vietnam
  val VU: Type = Value(247	, "VU") //	Vanuatu
  val WF: Type = Value(248	, "WF") //	Wallis and Futuna
  val WS: Type = Value(249	, "WS") //	Samoa
  val YE: Type = Value(250	, "YE") //	Yemen
  val YT: Type = Value(251	, "YT") //	Mayotte
  val ZA: Type = Value(252	, "ZA") //	South Africa
  val ZM: Type = Value(253	, "ZM") //	Zambia
  val ZW: Type = Value(254	, "ZW") //	Zimbabwe

  val JsonSerializer: EnumNameSerializer[CountryCodeId.type] = new EnumNameSerializer(this)
  implicit val jsonFormat: Format[Type] = makeJsonFormat[Type]

  implicit val listDefaultValueWriter: DefaultValueWriter[List[CountryCodeId.Type]] = DefaultValueWriter.listDefaultValueWriter[CountryCodeId.Type]()
}

