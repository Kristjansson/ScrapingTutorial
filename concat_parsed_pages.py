import glob
import codecs

def get_time(line):
    return line.split(',')[-1].strip()

file_names = glob.glob('parsed/*')

with codecs.open('final_table.csv', 'w', 'utf-8') as final_f:
    with codecs.open(file_names[0], 'r', 'utf-8') as first_f:
        for line in first_f:
            final_f.write(line)
        else:
            last_time = get_time(line)

    for idx, file_name in enumerate(file_names[1:]):
        with codecs.open(file_name, 'r', 'utf-8') as curr_f:
            curr_f_iter = curr_f.__iter__()
            next(curr_f_iter) # skip header row

            good_to_go = False
            for line in curr_f:
                # Use the time field to detect duplicates
                if (good_to_go or last_time > get_time(line)):
                    good_to_go = True
                    final_f.write(line)
                else:
                    print("dropped 1 line from ", file_name)
            else:
                last_time = get_time(line)
                
        if (idx % 100 == 0):
            print("\tCompleted ", file_name)