import re


def code_mapper(label):
    """
    Creates a dictionary for the given label as specified in 'I94_SAS_Labels_Descriptions.SAS'

    Parameters:
    label (str) : SAS label, can be one of 'i94cntyl', 'i94prtl', 'i94model', 'i94addrl'

    Returns:
    label_dict (dict) : Dictionary for label as specified in 'I94_SAS_Labels_Descriptions.SAS'

    """
    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        sas_labels_content = f.read()

    sas_labels_content = sas_labels_content.replace('\t', '')

    label_content = sas_labels_content[sas_labels_content.index(label):]
    label_content = label_content[:label_content.index(';')].split('\n')
    label_content = [i.replace("'", "") for i in label_content]

    label_dict = [i.split('=') for i in label_content[1:]]
    label_dict = dict([i[0].strip(), i[1].strip()] for i in label_dict if len(i) == 2)

    return label_dict


def process_cit_res(cit_res):
    """
    Processes cit_res dictionary so that its keys and values match those in immigration dataset.

    Parameters:
    cit_res (dict) : Unprocessed cit_res dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'

    Returns:
    (dict) : Mapping for i94cit and i94res columns' values

    """
    return {f'{k}.0': (v if not re.match('^INVALID:|^Collapsed|^No Country Code', v) else 'INVALID ENTRY')
            for k, v in cit_res.items()}


def process_mode(mode):
    """
    Processes mode dictionary so that its keys match those in immigration dataset.

    Parameters:
    mode (dict) : Unprocessed mode dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'

    Returns:
    (dict) : Mapping for i94mode column's values

    """
    return {f'{k}.0': v for k, v in mode.items()}


def format_state(s):
    """
    Formats the name of the state to comply with state names in other datasets.

    Parameters:
    s (string) : State name as specified in i94addrl of file 'I94_SAS_Labels_Descriptions.SAS'

    Returns:
    (str) : Formated state name

    """
    s = s.replace('DIST. OF', 'District of') \
         .replace('S.', 'South') \
         .replace('N.', 'North') \
         .replace('W.', 'West')
    return ' '.join([w.capitalize() if w != 'of' else w for w in s.split()])


def process_addr(addr):
    """
    Processes addr dictionary so that its values match those in immigration dataset.

    Parameters:
    addr (dict) : Unprocessed addr dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'

    Returns:
    (dict) : Mapping for i94addr column's values

    """
    return {k: format_state(v) for k, v in addr.items()}


def extract_port_state_code(port, addr):
    """
    Extracts port state codes from port dictionary values and creates a new dictionary with
    the same keys but with port state codes as values.

    Parameters:
    port (dict) : Unprocessed port dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'
    addr (dict) : Unprocessed addr dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'

    Returns:
    port_state_code (dict) : Dictionary with same keys as port but with port state codes as values

    """
    port_state_code = {}

    for k, v in port.items():
        if not re.match('^Collapsed|^No PORT Code', v):
            try:
                # extract state part from i94port
                # the state part contains the state and also other words
                state_part = v.rsplit(',', 1)[1]

                # create a set of all words in state part
                state_part_set = set(state_part.split())

                # if the state is valid (is in the set(i94addr.keys()), then retrieve state
                state = list(set(addr.keys()).intersection(state_part_set))[0]

                # add state to dict
                port_state_code[k] = state

            except IndexError:
                # no state is specified for Washington DC in labels so it is added here
                if v == 'WASHINGTON DC':
                    port_state_code[k] = 'DC'
                else:
                    port_state_code[k] = 'INVALID ENTRY'
        else:
            port_state_code[k] = 'INVALID ENTRY'

    return port_state_code


def extract_port_city(port, port_state_code):
    """
    Extracts port cities from port dictionary values and creates a new dictionary with
    the same keys but with port cities as values.

    Parameters:
    port (dict) : Unprocessed port dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'
    port_state_code (dict) : port_state_code dictionary as returned from extract_port_state_code function

    Returns:
    port_city (dict) : Dictionary with same keys as port but with port cities as values

    """
    port_city = {}
    for k, v in port.items():
        if port_state_code[k] == 'INVALID ENTRY':
            port_city[k] = 'INVALID ENTRY'
        else:
            # extract city part from i94port
            city_part = v.rsplit(',', 1)[0]

            # add city to dict
            port_city[k] = city_part

    return port_city


def process_port(port, addr):
    """
    Processes port dictionary so that two separate dictionaries are created. One for
    port states and another for port cities

    Parameters:
    port (dict) : Unprocessed port dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'
    addr (dict) : Unprocessed addr dictionary as specified in file 'I94_SAS_Labels_Descriptions.SAS'

    Returns:
    port_state_code (dict) : Dictionary with same keys as port but with port state codes as values
    port_city (dict) : Dictionary with same keys as port but with port cities as values

    """
    port_state_code = extract_port_state_code(port, addr)

    port_city = extract_port_city(port, port_state_code)

    return port_state_code, port_city


def process_sas_mappings():
    """
    Processes 'I94_SAS_Labels_Descriptions.SAS' file and returns the mappings for values in
    i94cit, i94res, i94port, i94mode, i94addr and i94visa columns of immigration dataset.

    Returns:
    cit_res (dict) : Mapping for i94cit and i94res columns' values
    port_state_code (dict) : Mapping for i94port column's values -state part-
    port_city (dict) : Mapping for i94port column's values -city part-
    mode (dict) : Mapping for i94mode column's values
    addr (dict) : Mapping for i94addr column's values
    visa (dict) : Mapping for i94visa column's values

    """
    cit_res = code_mapper('i94cntyl')
    port = code_mapper('i94prtl')
    mode = code_mapper('i94model')
    addr = code_mapper('i94addrl')
    visa = {'1.0': 'Business', '2.0': 'Pleasure', '3.0': 'Student'}

    cit_res = process_cit_res(cit_res)
    port_state_code, port_city = process_port(port, addr)
    mode = process_mode(mode)
    addr = process_addr(addr)

    return cit_res, port_state_code, port_city, mode, addr, visa


def main():
    """
    Prints dictionaries created by process_sas_mappings().
    For debug purpose only.

    """
    cit_res, port_state_code, port_city, mode, addr, visa = process_sas_mappings()

    print('-'*20)
    print(cit_res)
    print('-'*20)
    print(port_state_code)
    print('-'*20)
    print(port_city)
    print('-'*20)
    print(mode)
    print('-'*20)
    print(addr)
    print('-'*20)
    print(visa)


if __name__ == '__main__':
    main()
