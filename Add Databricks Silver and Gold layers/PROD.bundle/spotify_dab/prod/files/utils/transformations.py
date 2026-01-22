class reusable:
    
    '''
    columns_to_drop = ["col1", "col2", "col3"]
     df = df.drop(*columns_to_drop)

      * = list/tuple ko alag arguments me convert kar deta hai

    '''

    def dropColumn(self,df,columns):

        df = df.drop(*columns)
        return df

  