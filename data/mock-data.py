
class mock_data:
    def __init__(self,
                path,
                outputs_path = None,
                col_names_dict = None):
        
        self.path = path
        
        if outputs_path is None:
            self.outputs_path = self.path + '/' + 'out'
        else:
            self.outputs_path = outputs_path
        
        if col_names_dict is None:
            self.col_names_dict = {
                  'Date':'date',
                  'Hour':'hour', 
                  'Region1':'pcod',
                  'Region2': None,
                  'Count':'value' }
            
            self.timevar = self.col_names_dict['Date']
        # Otherwise specify dict manually
        else:
            self.col_names_dict = col_names_dict
        # Load data
        self.df = pd.read_csv(self.path)
        # Run aggregation
        self.agg = self.aggregation()
    
    def aggregation(self):
        # Aggregate indicator 5
        if self.col_names_dict['Region2'] != None:
            df_agg = self.df.groupby([self.col_names_dict['Region1'],self.col_names_dict['Region2']]).\
            apply(lambda x: pd.Series({ 
                            'mean':x[self.col_names_dict['Count']].mean(), 
                            'sd': x[self.col_names_dict['Count']].std()
                             })).reset_index() 
            
            
        else:
            # Aggregate indicator 1
            if self.col_names_dict['Hour'] != None:
                df_agg = self.df.groupby([self.col_names_dict['Region1'],self.timevar]).\
                apply(lambda x: pd.Series({ 
                            'mean':x[self.col_names_dict['Count']].mean(), 
                            'sd': x[self.col_names_dict['Count']].std()
                             })).reset_index()
                
            else:
                # Aggregate indicator 3
                df_agg = self.df.groupby([self.col_names_dict['Region1']]).\
                apply(lambda x: pd.Series({ 
                            'mean':x[self.col_names_dict['Count']].mean(), 
                            'sd': x[self.col_names_dict['Count']].std()
                             })).reset_index()
        
        return pd.merge(df_agg,self.df)
    
    def create_mock(self, mu, sd, clean = True):
        num_rows = self.agg.shape[0]
        # Create random variables following normal distribution 
        fraction = np.random.normal(mu,sd,num_rows)
        # Censor the values between -1 and 1
        fraction_censor = np.clip(fraction, -1, 1)
        self.agg['fraction'] = fraction_censor
        # Add fraction of sd up to 1 sd to the origial value
        self.agg['mock_value'] = self.agg[self.col_names_dict['Count']] + \
        self.agg['sd']*self.agg['fraction']
        # Take absolute integer value
        self.agg['mock_value'] = [round(num,0) for num in self.agg['mock_value'].abs()]
        
        # Only print original region, time variables and mock value
        if clean == True:
            # Print indicator 1
            if self.col_names_dict['Hour']:
                return self.agg[[self.col_names_dict['Region1'],self.col_names_dict['Date'],
                         self.col_names_dict['Hour'], 'mock_value']]
            else:
                # Print indicator 5
                if self.col_names_dict['Region2']:
                    return self.agg[[self.col_names_dict['Region1'],self.col_names_dict['Region2'],
                                     self.col_names_dict['Date'],'mock_value']]
                    
                else:
                    # Print indicator 3
                    return self.agg[[self.col_names_dict['Region1'],self.col_names_dict['Date'],
                         'mock_value']]
        
        # Print all variables in aggregation df
        else:
            return self.agg

        
        
if __name__ == "__main__":
    
    # create mock value for indicator 1
    test_ind1 = mock_data(path = "/path/to/location_event_counts_admin2.csv")
    mock_ind1 = test_ind1.create_mock(0.05,0.01,False)
    
    # create mock value for indicator 3
    test_ind3 = mock_data(path = "/path/to/unique_subscriber_counts_admin2.csv",
                 col_names_dict = {'Date':'date',
                  'Hour':None, 
                  'Region1':'pcod',
                  'Region2': None,
                  'Count':'value' })
    mock_ind3 = test_ind3.create_mock(0.05,0.01,False)
    
    # create mock value for indicator 5
    test_ind5 = mock_data(path = "/path/to/consecutive_trips_od_matrix_admin2.csv",
                 col_names_dict = {'Date':'date',
                  'Hour':None, 
                  'Region1':'pcod_from',
                  'Region2':'pcod_to',
                  'Count':'value' })
    mock_ind5 = test_ind5.create_mock(0.05,0.01, False)