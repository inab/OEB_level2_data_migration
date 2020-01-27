#!/usr/local/bin/python3
import typing
import subprocess
import os
import re
import sys
import subprocess

origids = {
    
    "EC":"QfO:2018-07-07_ECtest",
    "GO":"QfO:2018-07-07_GOtest",
    "G_STD2_Eukaryota":"QfO:2018-07-07_G_STD2_Eukaryota",
    "G_STD2_Fungi":"QfO:2018-07-07_G_STD2_Fungi",
    "G_STD2_Luca":"QfO:2018-07-07_G_STD2_LUCA",
    "G_STD2_Vertebrata":"QfO:2018-07-07_G_STD2_Vertebrata",
    "G_STD_Eukaryota":"QfO:2018-07-07_G_STD_Eukaryota",
    "G_STD_Fungi":"QfO:2018-07-07_G_STD_Fungi",
    "G_STD_Luca":"QfO:2018-07-07_G_STD_LUCA",
    "G_STD_Vertebrata":"QfO:2018-07-07_G_STD_Vertebrata",
    "STD_Bacteria":"QfO:2018-07-07_STD_Bacteria",
    "STD_Eukaryota":"QfO:2018-07-07_STD_Eukaryota",
    "STD_Fungi":"QfO:2018-07-07_STD_Fungi",
    "SwissTrees":"QfO:2018-07-07_SwissTree",
    "TreeFam-A":"QfO:2018-07-07_TreeFam-A"
    }

def replaceIdsforOrigIds(fname):
    for key,value in origids.items():
            subprocess.call("sed -i -e 's/'" + key +"'/'"+ value +"'/g' " + fname, shell=True)

for root, dirs, files in os.walk("."):
   for fname in files:
       if fname.endswith("json"):
            replaceIdsforOrigIds(fname)


        
        
    
