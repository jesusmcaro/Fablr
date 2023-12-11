from faker.providers import BaseProvider, DynamicProvider


class sample_dataframe_provider(BaseProvider):
    def __init__(self, faker):
       super().__init__(faker)
       self.generated_values = set()

    def sample_dataframe_provider_clear(self):
        self.generated_values = set()

    def sample_dataframe(self, df, column, unique: bool = False):
        if unique:
            return self.sample_and_append_to_set(df, column)
        elif not unique:
            return self.to_many_value(df, column)
        else:
            raise ValueError("unique must be a boolean")


    def to_many_value(self,df, column):
        elements = df[column].to_list()
        return self.random_elements(elements, length=1, unique = False)[0]

    def sample_and_append_to_set(self, df, column):
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

class sample_list_provider(BaseProvider):
    def __init__(self, faker):
       super().__init__(faker)
       self.generated_values = set()

    def sample_list_provider_clear(self):
        self.generated_values = set()

    def sample_list(self, list, unique: bool = False):
        if unique:
            return self.sample_and_append_to_set(list)
        elif not unique:
            return self.random_element(list)
        else:
            raise ValueError("unique must be a boolean")

    def sample_and_append_to_set(self, list):
        if len(self.generated_values) == len(list):
            self.sample_list_provider_clear()
            error_string = "Sampling rate reached, you are attempting to sample more times than the number of rows of the dataframe you are referencing."
            raise ValueError(error_string)
        value = self.random_element(list)
        while value in self.generated_values:
              value = self.random_element(list)
        self.generated_values.add(value)
        return value
