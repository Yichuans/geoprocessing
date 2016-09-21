for each in arcpy.mapping.ListBrokenDataSources(arcpy.mapping.MapDocument("CURRENT")):
    oldpath = each.dataSource
    newpath = oldpath.replace(r'D:', r'E:')
    each.findAndReplaceWorkspacePath(oldpath, newpath)