# Snippet from deleted code


@app.task(serializer='json')
def create_xml(loan_id, journey_api_user, journey_api_password, template_objects):
    """
        Can be deleted of the create_xml on cloudcode works eventually
    """
    loan = JourneyLoan(journey_api_user, journey_api_password)
    client = JourneyClient(journey_api_user, journey_api_password)
    branch = JourneyBranch(journey_api_user, journey_api_password)

    if template_objects is None:  # Dynamically fetch data
        try:
            loan.fetch(loan_id, to_embed=['client',
                                          'branch',
                                          'area',
                                          'compuscan',
                                          'loan_originator',
                                          'payout_method',
                                          'repayment_method',
                                          'repayment_bank_account',
                                          'product',
                                          'purpose',
                                          'agent'])
            client.fetch(loan.client_id, to_embed=['address_physical',
                                                   'employment',
                                                   'client_affordability',
                                                   'note'])
            branch.fetch(object_id=loan.branch_id, to_embed=['company'])
            # Fetch References
            references = client.fetch_related('reference')

            if getattr(client, 'address_postal', None) is None:
                client.address_postal = {}
            if getattr(client, 'address_physical', None) is None:
                client.address_physical = {}

        except Exception as e:
            logger.error('Error fetching data dynamically: ' + str(e))
            raise Exception('Error fetching data dynamically: ' + str(e))
    else:  # Load data from blob
        try:
            loan.load(template_objects['loan'])
            client.load(template_objects['client'])
            branch.load(template_objects['branch'])

        except Exception as e:
            logger.error('Error parsing blob: ' + str(e))
            raise Exception('Error parsing blob: ' + str(e))

        # Load References
        try:
            references = []
            for dict_ref in template_objects['references']:
                journey_ref = JourneyReference(journey_api_user, journey_api_password)
                journey_ref.load(dict_ref)
                references.append(journey_ref)
        except Exception as e:
            logger.error('Error loading references: ' + str(e))
            raise Exception('Error loading references: ' + str(e))


    # Check for mandatory information
    req_on_loan = ['loan_number', 'date_paidout']
    for i in req_on_loan:
        if not getattr(loan, i, None):
            logger.error('Required value on loan not found: %s' % i)
            logger.error(loan)
            raise Exception('Required value on loan not found: %s' % i)

    try:
        # Get datamodel
        data_model = json.loads(open(settings.BASE_DIR + '/templates/journey_datamodel.json').read())

        address_province_types = data_model['models']['address']['fields']['province']['options']
        address_province = address_province_types[client.address_physical.province]['display']
        marital_status_types = data_model['models']['client']['fields']['marital_status']['options']
        if getattr(client, 'marital_status', None) is not None:
            marital_status = marital_status_types[client.marital_status]
        else:
            marital_status = ''

        client_created_date = parse(client.date_created).strftime('%Y%m%d') if loan.date_created is not None else ''
        loan_origination_date = parse(loan.date_created).strftime('%Y%m%d') if loan.date_created is not None else ''
        paidout_date = parse(loan.date_paidout).strftime('%Y%m%d') if loan.date_paidout is not None else ''

        try:
            note_date = parse(client.note.date_created)
            note_date = note_date.strftime('%Y%m%d')
        except Exception:
            note_date = ''

        reference_relation_options = data_model['models']['reference']['fields']['relation']['options']
        if not loan.repayment_method.default_date_adj:
            payday_shift = "B"
        elif loan.repayment_method.default_date_adj['key'] == 1:
            payday_shift = "B"
        else:
            payday_shift = "A"

        allps_paymentstream_options = data_model['models']['repayment_method']['fields']['allps_pmt_stream']['options']
        allps_paymentstream_string = loan.repayment_method.allps_pmt_stream if loan.repayment_method.allps_pmt_stream else ''
        payout_method_type = payment_method_types[loan.payout_method.payout_type]
        repayment_method_type = payment_method_types[loan.repayment_method.repayment_type]

        try:
            bank_account_type = bank_account_types[loan.repayment_bank_account.acc_type]
        except Exception:
            bank_account_type = None

        loan_cost = loan.product.all_instalment_service_exc_vat + loan.product.all_instalment_initiation_exc_vat
        if loan.product.all_instalment_discount_exc_vat is not None:
            loan_cost += loan.product.all_instalment_discount_exc_vat
        instalment_frequency_types = data_model['models']['product']['fields']['instalment_frequency']['options']

        allps_platform_id = ''
        if loan.repayment_method.allps_pmt_stream in ['NAEDO', 'EFT', 'SEFT']:
            allps_platform_id = 3
        elif loan.repayment_method.allps_pmt_stream in ['AEDO', ]:
            allps_platform_id = 2

        loan_repayment_method = ''
        if loan.repayment_method.repayment_type != 1:
            loan_repayment_method = 'CASH'
        elif loan.repayment_method.allps_pmt_stream is not None:
            loan_repayment_method = 'ALLPS-I'
        else:
            loan_repayment_method = 'BANK TRANSFER'

    except Exception as e:
        logger.error(str(e))
        raise Exception(str(e))

    # Format addresses as a single string so that it renders correctly in proloan
    client_postal_address = ''
    if client.address_postal:
        if client.address_postal.line_1:
            client_postal_address += client.address_postal.line_1
        if client.address_postal.city:
            client_postal_address += '\n' + client.address_postal.city
        client_postal_address += '\n' + address_province

    client_street_address = ''
    if client.address_physical:
        if client.address_physical.line_1:
            client_street_address += client.address_physical.line_1
        if client.address_physical.city:
            client_street_address += '\n' + client.address_physical.city
        client_street_address += '\n' + address_province

    try:
        setattr(loan, 'client', client)
        context = {
            'loan': loan,
            'branch': branch,
            'datamodel': data_model,
            'client_postal_address': client_postal_address,
            'client_street_address': client_street_address,
            'loan_origination_date': loan_origination_date,
            'instalment_frequency': instalment_frequency_types[loan.product.instalment_frequency],
            'address_province': address_province,
            'client_created_date': client_created_date,
            'paidout_date': paidout_date,
            'note_date': note_date,
            'loan_cost': loan_cost,
            'marital_status': marital_status,
            'payday_shift': payday_shift,
            'payout_method_code': payout_method_type,
            'repayment_method_code': repayment_method_type,
            'bank_account_type': bank_account_type,
            'allps_platform_id': allps_platform_id,
            'allps_paymentstream': allps_paymentstream_string,
            'relation_types': reference_relation_options,
            'references': references,
            'loan_repayment_method': loan_repayment_method
        }
        # Write XML to file
        dir = '%s/%s/%s/' % (settings.XML_STORAGE_LOCATION, branch.company.id, branch.id)
        ensure_dir_exists(dir)
        path = '%s/%s.xml' % (dir, loan.id)
        # Render XML from template
        pretty_print = lambda data: '\n'.join(
            [line for line in parseString(data).toprettyxml(indent=' ' * 2).split('\n') if line.strip()])
        template = loader.get_template('proloan_import.xml')
        xml_string = template.render(context)
        xml_string_encoded = xml_string
        xml_parsed = pretty_xml.parseString(xml_string_encoded)
        xml_pretty = xml_parsed.toprettyxml()
        final_xml = pretty_print(xml_pretty)  # Removes blank lines
        # If XML exists log a note
        if os.path.isfile(path):
            logger.info("XML already exists and will be updated now.")

        # Write XML to file
        with open(path, 'wb') as f:
            f.write(final_xml.encode('ascii', 'ignore'))  # might encode to byte since python 3.7
            f.close()
            utils.set_permissions(path)
            logger.info("XML successfully saved to file: " + path)
    except Exception as e:
        logger.error('Error rendering XML template: ' + str(e))
        raise Exception('Error rendering XML template: ' + str(e))
