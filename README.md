# DeterminantsOfPhysicalActivity

This repo contains the code and results used for the paper "Utilizing large-scale human mobility data to identify determinants of physical activity", accepted for publication in Scientific Reports. 

The data supporting the findings of this study are available from Veraset, Inc. (provided upon request submitted at https://www.veraset.com). Instead of splitting the days based on a universal UTC-timestamp like Veraset does, we wrote local_date.py, which splits the days based on the local timestamp of each user. This was necessary for our analysis, since we were interested in visits on exercise and non-exercise days for each user. All our analyses utilize the visits based on the local dates of the users.
