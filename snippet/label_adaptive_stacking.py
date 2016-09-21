def FindLabel([en_name]):
    # [[en_name]] is the esri way of referring field, needs to be replaced for production
    LENGTH = 15
    breaks = len([en_name])/LENGTH

    if breaks ==0:
        return [en_name]

    chunk_list = [en_name].split(' ')
    chunk_list_size = len(chunk_list)
    avg_exp_len = len([en_name]) * 1.0 / breaks

    # debug ========
    # print 'breaks:', breaks
    # print 'chunk_list:', chunk_list
    # print 'item size', map(len, chunk_list)
    # print 'avg_len', avg_exp_len


    # if space/natual word breaks are less than the breaks needed, don't split
    if chunk_list_size < breaks:
        return [en_name]

    # if equal, then split every item
    elif chunk_list_size == breaks:
        return '\n'.join(chunk_list)

    # chunk_list_size > breaks
    else:
        label_list = []

        label_line = ''
        # most likely name + designation (multiple)
        # if breaks == 1:

        i = 0
        while i <= chunk_list_size-1:
            # the difference between is small, break
            while len(label_line) < avg_exp_len and i <= chunk_list_size -1:
                label_line += chunk_list[i] + ' '
                i += 1
            
            label_list.append(label_line.strip())

            # decrease break number
            breaks -= 1

            # if all breaks are done, append all the rest to the next line
            if breaks == 0:
                label_line = ' '.join(chunk_list[i:])
                label_list.append(label_line)
                break

            # reset label_line
            label_line = ''

        # debug ============
        print label_list

        return '\n'.join(label_list)

