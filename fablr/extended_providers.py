from faker.providers import BaseProvider

class ToManyProvider(BaseProvider):
    def ToManyValue(self,df, column):
        return df[column][self.random_int(min = 0,
                                          max = len(df[column])-1)]

class ToOneProvider(BaseProvider):
    def __init__(self, faker):
       super().__init__(faker)
       self.generated_values = set()
       
    def ToOneProviderClear(self):
        self.generated_values = set()
       
    def ToOneValue(self, df, column):
        if len(self.generated_values) == len(df[column]):
            error_string = "Sampling rate reached, you are attempting to sample more times than the number of rows of the dataframe you are referencing."
            raise ValueError(error_string)
        value = df[column][self.random_int(min=0,
                                              max=len(df[column])-1)]
        while value in self.generated_values:
           value = df[column][self.random_int(min=0,
                                              max=len(df[column])-1)]
        self.generated_values.add(value)
        return value
    
    