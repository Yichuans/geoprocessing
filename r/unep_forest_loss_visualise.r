library("Cairo", lib.loc="C:/Users/yichuans/Documents/R/win-library/3.0")
library(RODBC)
library(ggplot2)
library(scales)
library(sqldf)


setwd('D:/Yichuan/BrianO/UNEP-report/analysis')

# connect to postgres database where the result is held
myconn = odbcConnect('wh')

unep_loss_year = sqlFetch(myconn, 'ad_hoc.unep_afr_loss_year')
unep_loss_year_biome = sqlFetch(myconn, 'ad_hoc.unep_afr_loss_year_biome')
unep_loss_year_country = sqlFetch(myconn, 'ad_hoc.unep_afr_loss_year_country')
close(myconn)

# forest loss year total
a = ggplot(unep_loss_year, aes(x=as.factor(year+2000), y=(total_area_km2))) +
  geom_bar(stat = 'identity', width=0.3) +
  geom_point(aes(y=cumsum(total_area_km2)), colour='red') +
  geom_line(aes(y=cumsum(total_area_km2), group=1), colour='red') +
  scale_y_continuous(labels=comma) +
  xlab('Loss year')+
  ylab('Area (km2)')+
  ggtitle('Total forest loss 2001-2013 in Africa')

cairo_pdf(file='unep_afr_loss_year.pdf', width=8.267, height=11, onefile=T)
print(a)
dev.off()

# biome
b = ggplot(unep_loss_year_biome, aes(x=as.factor(year+2000), y=(total_area_km2))) +
  geom_bar(stat = 'identity', width=0.3) +
  facet_grid(biome_name~.)+
  #geom_point(aes(y=cumsum(total_area_km2)), colour='red') + 
  #geom_line(aes(y=cumsum(total_area_km2), group=biome_name), colour='red') +
  scale_y_continuous(labels=comma) +
  xlab('Loss year')+
  ylab('Area (km2)')+
  ggtitle('Total forest loss 2001-2013 in Africa by biome')

## calculate number of biomes
num_biome = sqldf('select count(distinct biome) from unep_loss_year_biome')[1,1]
cairo_pdf(file='unep_loss_year_biome.pdf', width=8.267, height=2*num_biome, onefile=T)
print(b)
dev.off()

# country
c = ggplot(unep_loss_year_country, aes(x=as.factor(year+2000), y=(total_area_km2))) +
  geom_bar(stat = 'identity', width=0.3) +
  facet_grid(terr_name~.)+
  #geom_point(aes(y=cumsum(total_area_km2)), colour='red') + 
  #geom_line(aes(y=cumsum(total_area_km2), group=biome_name), colour='red') +
  scale_y_continuous(labels=comma) +
  xlab('Loss year')+
  ylab('Area (km2)')+
  ggtitle('Total forest loss 2001-2013 in Africa by country')

num_country = sqldf('select count(distinct iso3_code) from unep_loss_year_country')[1,1]
cairo_pdf(file='unep_loss_year_country.pdf', width=8.267, height=2*num_country, onefile=T)
print(c)
dev.off()
