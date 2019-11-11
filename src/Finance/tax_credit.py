def tax_credit(income=52594, fam_size=3, lowest_2 = 7954):
    '''
    Calculates the tax credit for the enrollee for their income status
    and location.  The threshold and cutoffs for each level are set 
    annually by congress as part of the HHS budget bill so this will need to be
    updated in 2020 after the next version is approved.
     '''
     
    poverty_threshold = 12490 + 4420*(fam_size-1)
    percent_poverty = income/poverty_threshold
    if  percent_poverty <= 1.33:
        rate = .0206
    elif percent_poverty <= 1.5:
        rate = .0309+(((percent_poverty-1.33)/(1.5-1.33))*(.0412-.0309))
    elif percent_poverty <= 2:
        rate = .0412+(((percent_poverty-1.5)/(2-1.5))*(.0649-.0412))
    elif percent_poverty <= 2.5:
        rate = .0649+(((percent_poverty-2)/(2.5-2))*(.0829-.0649))
    elif percent_poverty <= 3:
        rate = .0829+(((percent_poverty-2.5)/(3-2.5))*(.0978-.0829))
    elif percent_poverty <= 4:
        rate = .0978
    else:
        return 0
        
    return lowest_2*(1-rate)
   
